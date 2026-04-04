import { useState, useCallback } from 'react';
import { uploadFile as apiUploadFile } from '../api/uploads.js';

export function useFileUpload() {
  const [isUploading, setIsUploading] = useState(false);
  const [uploadError, setUploadError] = useState(null);
  const [lastUpload, setLastUpload] = useState(null);

  const uploadFile = useCallback(async (file) => {
    setIsUploading(true);
    setUploadError(null);

    try {
      const response = await apiUploadFile(file);
      setLastUpload(response);
    } catch (err) {
      setUploadError(err.message);
    } finally {
      setIsUploading(false);
    }
  }, []);

  const clearUploadError = useCallback(() => {
    setUploadError(null);
  }, []);

  const clearLastUpload = useCallback(() => {
    setLastUpload(null);
  }, []);

  return { isUploading, uploadError, lastUpload, uploadFile, clearUploadError, clearLastUpload };
}
