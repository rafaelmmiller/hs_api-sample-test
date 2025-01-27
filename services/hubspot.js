const hubspot = require('@hubspot/api-client');

let hubspotClientInstance;

const getHubspotClient = (accessToken) => {
  if (!hubspotClientInstance) {
    hubspotClientInstance = new hubspot.Client({ accessToken: accessToken ?? '' });
  }
  return hubspotClientInstance;
};

module.exports = {
  getHubspotClient
};
