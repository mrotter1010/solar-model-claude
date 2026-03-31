import { useState, useCallback } from 'react';
import { uploadFile as apiUploadFile } from '../api/uploads.js';

export function useFileUpload() {
  const [isUploading, setIsUploading] = useState(false);
  const [uploadError, setUploadError] = useState(null);
  const [lastUpload, setLastUpload] = useState(null);

  const uploadFile = useCallback(async (file, fileType) => {
    setIsUploading(true);
    setUploadError(null);

    try {
      const response = await apiUploadFile(file, fileType);
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

  return { isUploading, uploadError, lastUpload, uploadFile, clearUploadError };
}
