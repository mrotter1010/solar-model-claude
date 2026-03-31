import { CONFIG } from '../config.js';

const BASE = CONFIG.analysisApiUrl;

/**
 * POST /uploads/{fileType} — Upload a file to the analysis API
 * @param {File} file — File object from input
 * @param {'rate'|'kmz'|'load-profile'} fileType
 * @returns {Promise<{file_type: string, filename: string, path: string, size_bytes: number}>}
 */
export async function uploadFile(file, fileType) {
  const formData = new FormData();
  formData.append('file', file);

  const headers = {};
  if (CONFIG.analysisApiKey) {
    headers['X-API-Key'] = CONFIG.analysisApiKey;
  }

  const res = await fetch(`${BASE}/uploads/${fileType}`, {
    method: 'POST',
    headers,
    body: formData,
  });
  if (!res.ok) throw new Error(`Upload failed: ${res.status} ${res.statusText}`);
  return res.json();
}
