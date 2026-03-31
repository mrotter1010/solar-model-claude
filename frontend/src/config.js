export const CONFIG = {
  orchestratorUrl: import.meta.env.VITE_ORCHESTRATOR_URL || 'http://localhost:8001',
  analysisApiUrl: import.meta.env.VITE_ANALYSIS_API_URL || 'http://localhost:8000',
  analysisApiKey: import.meta.env.VITE_ANALYSIS_API_KEY || '',
};
