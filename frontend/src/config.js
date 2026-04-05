export const CONFIG = {
  orchestratorUrl: import.meta.env.VITE_ORCHESTRATOR_URL || '/orchestrator',
  analysisApiUrl: import.meta.env.VITE_ANALYSIS_API_URL || '/api',
  analysisApiKey: import.meta.env.VITE_ANALYSIS_API_KEY || '',
};
