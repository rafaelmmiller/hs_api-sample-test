let hubspotClientInstance;

export const getHubspotClient = () => {
  if (!hubspotClientInstance) {
    hubspotClientInstance = new hubspot.Client({ accessToken: '' });
  }
  return hubspotClientInstance;
};