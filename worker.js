const hubspot = require('@hubspot/api-client');
const { queue } = require('async');
const _ = require('lodash');

const { filterNullValuesFromObject, goal, generateLastModifiedDateFilter } = require('./utils');
const Domain = require('./Domain');
const { getHubspotClient } = require('./services/hubspot');
const { error, warn, info, debug } = require('./logger');
const propertyPrefix = 'hubspot__';
let expirationDate;

const saveDomain = async domain => {
  // disable this for testing purposes
  return;

  try {
    domain.markModified('integrations.hubspot.accounts');
    await domain.save();
  } catch (err) {
    error('Failed to save domain', { error: err });
    // Re-throw to allow caller to handle
    throw err;
  }
};

/**
 * Get access token from HubSpot
 */
const refreshAccessToken = async (domain, hubId, tryCount) => {
  const { HUBSPOT_CID, HUBSPOT_CS } = process.env;
  const account = domain.integrations.hubspot.accounts.find(account => account.hubId === hubId);
  const { accessToken, refreshToken } = account;

  const hubspotClient = getHubspotClient();

  return hubspotClient.oauth.tokensApi
    .createToken('refresh_token', undefined, undefined, HUBSPOT_CID, HUBSPOT_CS, refreshToken)
    .then(async result => {
      const body = result.body ? result.body : result;

      const newAccessToken = body.accessToken;
      expirationDate = new Date(body.expiresIn * 1000 + new Date().getTime());

      hubspotClient.setAccessToken(newAccessToken);
      if (newAccessToken !== accessToken) {
        account.accessToken = newAccessToken;
      }

      info('Access token refreshed', { hubId, newAccessToken });

      return true;
    });
};

/**
 * Get recently modified companies as 100 companies per page
 */
const processCompanies = async (domain, hubId, q) => {
  const hubspotClient = getHubspotClient();

  const account = domain.integrations.hubspot.accounts.find(account => account.hubId === hubId);
  const lastPulledDate = new Date(account.lastPulledDates.companies);
  const now = new Date();

  let hasMore = true;
  const offsetObject = {};
  const limit = 100;

  while (hasMore) {
    const lastModifiedDate = offsetObject.lastModifiedDate || lastPulledDate;
    const lastModifiedDateFilter = generateLastModifiedDateFilter(lastModifiedDate, now);
    const searchObject = {
      filterGroups: [lastModifiedDateFilter],
      sorts: [{ propertyName: 'hs_lastmodifieddate', direction: 'ASCENDING' }],
      properties: [
        'name',
        'domain',
        'country',
        'industry',
        'description',
        'annualrevenue',
        'numberofemployees',
        'hs_lead_status'
      ],
      limit,
      after: offsetObject.after
    };

    let searchResult = {};

    let tryCount = 0;
    while (tryCount <= 4) {
      try {
        searchResult = await hubspotClient.crm.companies.searchApi.doSearch(searchObject);
        break;
      } catch (err) {
        tryCount++;

        if (new Date() > expirationDate) await refreshAccessToken(domain, hubId);

        warn(`Retrying to fetch companies, attempt ${tryCount}`, { hubId, error: err });

        await new Promise((resolve, reject) => setTimeout(resolve, 5000 * Math.pow(2, tryCount)));
      }
    }

    if (!searchResult) throw new Error('Failed to fetch companies for the 4th time. Aborting.');

    const data = searchResult?.results || [];
    offsetObject.after = parseInt(searchResult?.paging?.next?.after);

    info('Fetched company batch', { hubId, batchSize: data.length });

    data.forEach(company => {
      if (!company.properties) return;

      const actionTemplate = {
        includeInAnalytics: 0,
        companyProperties: {
          company_id: company.id,
          company_domain: company.properties.domain,
          company_industry: company.properties.industry
        }
      };

      const isCreated = !lastPulledDate || (new Date(company.createdAt) > lastPulledDate);

      q.push({
        actionName: isCreated ? 'Company Created' : 'Company Updated',
        actionDate: new Date(isCreated ? company.createdAt : company.updatedAt) - 2000,
        ...actionTemplate
      });
    });

    if (!offsetObject?.after) {
      hasMore = false;
      break;
    } else if (offsetObject?.after >= 9900) {
      offsetObject.after = 0;
      offsetObject.lastModifiedDate = new Date(data[data.length - 1].updatedAt).valueOf();
    }
  }

  account.lastPulledDates.companies = now;
  await saveDomain(domain);

  return true;
};

/**
 * Get recently modified contacts as 100 contacts per page
 */
const processContacts = async (domain, hubId, q) => {
  const hubspotClient = getHubspotClient();

  const account = domain.integrations.hubspot.accounts.find(account => account.hubId === hubId);
  const lastPulledDate = new Date(account.lastPulledDates.contacts);
  const now = new Date();

  let hasMore = true;
  const offsetObject = {};
  const limit = 100;

  while (hasMore) {
    const lastModifiedDate = offsetObject.lastModifiedDate || lastPulledDate;
    const lastModifiedDateFilter = generateLastModifiedDateFilter(lastModifiedDate, now, 'lastmodifieddate');
    const searchObject = {
      filterGroups: [lastModifiedDateFilter],
      sorts: [{ propertyName: 'lastmodifieddate', direction: 'ASCENDING' }],
      properties: [
        'firstname',
        'lastname',
        'jobtitle',
        'email',
        'hubspotscore',
        'hs_lead_status',
        'hs_analytics_source',
        'hs_latest_source'
      ],
      limit,
      after: offsetObject.after
    };

    let searchResult = {};

    let tryCount = 0;
    while (tryCount <= 4) {
      try {
        searchResult = await hubspotClient.crm.contacts.searchApi.doSearch(searchObject);
        break;
      } catch (err) {
        tryCount++;

        if (new Date() > expirationDate) await refreshAccessToken(domain, hubId);

        warn(`Retrying to fetch contacts, attempt ${tryCount}`, { hubId, error: err });

        await new Promise((resolve, reject) => setTimeout(resolve, 5000 * Math.pow(2, tryCount)));
      }
    }

    if (!searchResult) throw new Error('Failed to fetch contacts for the 4th time. Aborting.');

    const data = searchResult.results || [];

    info('Fetched contact batch', { hubId, batchSize: data.length });

    offsetObject.after = parseInt(searchResult.paging?.next?.after);
    const contactIds = data.map(contact => contact.id);

    // contact to company association
    const contactsToAssociate = contactIds;
    let companyAssociationsResults = [];
    try {
      const response = await hubspotClient.apiRequest({
        method: 'post',
        path: '/crm/v3/associations/CONTACTS/COMPANIES/batch/read',
        body: { inputs: contactsToAssociate.map(contactId => ({ id: contactId })) }
      });
      companyAssociationsResults = (await response.json())?.results || [];
    } catch (err) {
      error('Failed to fetch company associations', { hubId, error: err });
      companyAssociationsResults = [];
    }

    const companyAssociations = Object.fromEntries(companyAssociationsResults.map(a => {
      if (a.from) {
        contactsToAssociate.splice(contactsToAssociate.indexOf(a.from.id), 1);
        return [a.from.id, a.to[0].id];
      } else return false;
    }).filter(x => x));

    data.forEach(contact => {
      if (!contact.properties || !contact.properties.email) return;

      const companyId = companyAssociations[contact.id];

      const isCreated = new Date(contact.createdAt) > lastPulledDate;

      const userProperties = {
        company_id: companyId,
        contact_name: ((contact.properties.firstname || '') + ' ' + (contact.properties.lastname || '')).trim(),
        contact_title: contact.properties.jobtitle,
        contact_source: contact.properties.hs_analytics_source,
        contact_status: contact.properties.hs_lead_status,
        contact_score: parseInt(contact.properties.hubspotscore) || 0
      };

      const actionTemplate = {
        includeInAnalytics: 0,
        identity: contact.properties.email,
        userProperties: filterNullValuesFromObject(userProperties)
      };

      q.push({
        actionName: isCreated ? 'Contact Created' : 'Contact Updated',
        actionDate: new Date(isCreated ? contact.createdAt : contact.updatedAt),
        ...actionTemplate
      });
    });

    if (!offsetObject?.after) {
      hasMore = false;
      break;
    } else if (offsetObject?.after >= 9900) {
      offsetObject.after = 0;
      offsetObject.lastModifiedDate = new Date(data[data.length - 1].updatedAt).valueOf();
    }
  }

  account.lastPulledDates.contacts = now;
  await saveDomain(domain);

  return true;
};

/**
 * Get recently modified meetings
 */
const processMeetings = async (domain, hubId, q) => {
  const hubspotClient = getHubspotClient();
  const account = domain.integrations.hubspot.accounts.find(account => account.hubId === hubId);
  
  // workaround to get the lastPulledDate for meetings. Doesn't have a get() method.
  const lastPulledDates = JSON.parse(JSON.stringify(account.lastPulledDates));
  const lastPulledDate = new Date(lastPulledDates.meetings);
  const now = new Date();

  let hasMore = true;
  const offsetObject = {};
  const limit = 100;

  while (hasMore) {

    const searchObject = {
      // removed filterGroups. Not necessary. It was throwing an 400 error.
      // filterGroups: [lastModifiedDateFilter],
      sorts: [{ propertyName: 'hs_lastmodifieddate', direction: 'ASCENDING' }],
      properties: [
        'hs_meeting_title',
        'hs_createdate',
        'hs_lastmodifieddate',
        'hs_object_id'
      ],
      limit,
      after: offsetObject.after
    }

    let searchResult = {};

    let tryCount = 0;
    while (tryCount <= 4) {
      try {
        searchResult = await hubspotClient.crm.objects.meetings.searchApi.doSearch(searchObject);
        break;
      } catch (err) {
        tryCount++;

        if (new Date() > expirationDate) {
          await refreshAccessToken(domain, hubId);
        }

        warn(`Retrying to fetch meetings, attempt ${tryCount}`, { hubId, error: err });

        await new Promise((resolve, reject) => setTimeout(resolve, 5000 * Math.pow(2, tryCount)));
      }
    }

    if (!searchResult) {
      throw new Error('Failed to fetch meetings for the 4th time. Aborting.');
    }

    const data = searchResult.results || [];

    info('Fetched meeting batch', { hubId, batchSize: data.length });

    offsetObject.after = parseInt(searchResult.paging?.next?.after);
    const meetingIds = data.map(meeting => meeting.id);

    // contact to meeting association
    const meetingsToAssociate = meetingIds;
    let contactAssociationsResults = [];
    try {
      const response = await hubspotClient.apiRequest({
        method: 'post',
        path: '/crm/v3/associations/MEETINGS/CONTACTS/batch/read',
        body: { inputs: meetingsToAssociate.map(meetingId => ({ id: meetingId })) }
      });
      contactAssociationsResults = (await response.json())?.results || [];
    } catch (err) {
      error('Failed to fetch meeting associations', { hubId, error: err });
      contactAssociationsResults = [];
    }
    const contactAssociations = Object.fromEntries(contactAssociationsResults.map(a => {
      if (a.from) {
        meetingsToAssociate.splice(meetingsToAssociate.indexOf(a.from.id), 1);
        return [a.from.id, a.to[0].id];
      } else return false;
    }).filter(x => x));
    // get contact emails
    const contactEmails = await Promise.all(Object.values(contactAssociations).map(async contactId => {
      try {
        const contact = await hubspotClient.crm.contacts.basicApi.getById(contactId, ['email']);
        return { id: contactId, email: contact.properties.email };
      } catch (err) {
        error('Failed to fetch contact email', { hubId, contactId, error: err });
        return { id: contactId, email: null };
      }
    }));

    const contactEmailMap = Object.fromEntries(contactEmails.map(contact => [contact.id, contact.email]));

    data.forEach(meeting => {
      if (!meeting.properties) {
        return;
      }

      const contactEmail = contactEmailMap[contactAssociations[meeting.id]];

      const isCreated = new Date(meeting.createdAt) > lastPulledDate;
      const meetingProperties = {
        contact_email: contactEmail,
        meeting_title: meeting.properties.hs_meeting_title,
        meeting_start_time: meeting.properties.hs_createdate,
        meeting_end_time: meeting.properties.hs_lastmodifieddate,
        meeting_object_id: meeting.properties.hs_object_id
      };

      const actionTemplate = {
        includeInAnalytics: 0,
        identity: contactEmail,
        meetingProperties: filterNullValuesFromObject(meetingProperties)
      };

      q.push({
        actionName: isCreated ? 'Meeting Created' : 'Meeting Updated',
        actionDate: new Date(isCreated ? meeting.createdAt : meeting.updatedAt),
        ...actionTemplate
      });
    });

    if (!offsetObject?.after) {
      hasMore = false;
      break;
    } else if (offsetObject?.after >= 9900) {
      offsetObject.after = 0;
      offsetObject.lastModifiedDate = new Date(data[data.length - 1].updatedAt).valueOf();
    }
  }

  account.lastPulledDates.meetings = now;
  await saveDomain(domain);

  return true;
};


const createQueue = (domain, actions) => queue(async (action, callback) => {
  actions.push(action);

  if (actions.length > 2000) {
    info('Inserting actions to database', { apiKey: domain.apiKey, count: actions.length });

    const copyOfActions = _.cloneDeep(actions);
    actions.splice(0, actions.length);

    goal(copyOfActions);
  }

  callback();
}, 100000000);

const drainQueue = async (domain, actions, q) => {
  if (q.length() > 0) await q.drain();

  if (actions.length > 0) {
    info('Draining remaining actions to database', { apiKey: domain.apiKey, count: actions.length });
    goal(actions)
  }

  return true;
};

const pullDataFromHubspot = async () => {
  info('Start pulling data from HubSpot');

  const domain = await Domain.findOne({});

  for (const account of domain.integrations.hubspot.accounts) {
    info('Start processing account', { hubId: account.hubId });

    try {
      await refreshAccessToken(domain, account.hubId);
    } catch (err) {
      error(err, { apiKey: domain.apiKey, metadata: { operation: 'refreshAccessToken' } });
    }

    info('Refreshed access token', { hubId: account.hubId });

    const actions = [];
    const q = createQueue(domain, actions);

    // commented out for testing purposes
    // try {
    //   await processContacts(domain, account.hubId, q);
    //   info('Processed contacts', { hubId: account.hubId });
    // } catch (err) {
    //   error(err, { apiKey: domain.apiKey, metadata: { operation: 'processContacts', hubId: account.hubId } });
    // }

    // commented out for testing purposes
    // try {
    //   await processCompanies(domain, account.hubId, q);
    //   info('Processed companies', { hubId: account.hubId });
    // } catch (err) {
    //   error(err, { apiKey: domain.apiKey, metadata: { operation: 'processCompanies', hubId: account.hubId } });
    // }

    try {
      await processMeetings(domain, account.hubId, q);
      info('Processed meetings', { hubId: account.hubId });
    } catch (err) {
      error(err, { apiKey: domain.apiKey, metadata: { operation: 'processMeetings', hubId: account.hubId } });
    }

    try {
      await drainQueue(domain, actions, q);
      info('Drained queue', { hubId: account.hubId });
    } catch (err) {
      error(err, { apiKey: domain.apiKey, metadata: { operation: 'drainQueue', hubId: account.hubId } });
    }

    await saveDomain(domain);

    info('Finished processing account', { hubId: account.hubId });
  }

  process.exit();
};

module.exports = pullDataFromHubspot;
