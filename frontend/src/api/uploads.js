import { CONFIG } from '../config.js';

const BASE = CONFIG.analysisApiUrl;

/**
 * POST /uploads — Upload a file with automatic type detection
 * @param {File} file — File object from input
 * @returns {Promise<{file_type: string, filename: string, path: string, size_bytes: number, detected: boolean, confidence: string|null, message: string|null, extracted_text: string|null}>}
 */
export async function uploadFile(file) {
  const formData = new FormData();
  formData.append('file', file);

  const headers = {};
  if (CONFIG.analysisApiKey) {
    headers['X-API-Key'] = CONFIG.analysisApiKey;
  }

  const res = await fetch(`${BASE}/uploads`, {
    method: 'POST',
    headers,
    body: formData,
  });
  if (!res.ok) throw new Error(`Upload failed: ${res.status} ${res.statusText}`);
  return res.json();
}
