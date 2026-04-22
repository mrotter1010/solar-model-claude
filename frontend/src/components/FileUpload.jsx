import { useRef, forwardRef, useImperativeHandle } from 'react';
import { Paperclip, X, Loader2 } from 'lucide-react';

const FileUpload = forwardRef(function FileUpload(
  { onUpload, isUploading, uploadError, lastUpload, onClearError },
  ref
) {
  const fileInputRef = useRef(null);

  // Expose drop handler so parent can forward drag-and-drop events
  useImperativeHandle(ref, () => ({
    handleFileDrop: (file) => {
      if (!isUploading) {
        onUpload(file);
      }
    },
  }));

  const handleFileSelect = (e) => {
    const file = e.target.files[0];
    if (file && !isUploading) {
      onUpload(file);
    }
    // Reset so the same file can be re-selected
    e.target.value = '';
  };

  return (
    <div className="relative flex items-center gap-1.5">
      <button
        type="button"
        onClick={() => fileInputRef.current?.click()}
        className="rounded-lg p-2 text-vantyra-text-s hover:text-vantyra-text hover:bg-vantyra-bg-h disabled:opacity-50"
        disabled={isUploading}
        title="Attach file"
      >
        {isUploading ? (
          <Loader2 size={18} className="animate-spin" />
        ) : (
          <Paperclip size={18} />
        )}
      </button>

      {/* Upload error chip */}
      {uploadError && (
        <span className="inline-flex items-center gap-1 rounded-full bg-red-950/40 border border-red-800/50 px-2 py-0.5 text-xs text-vantyra-error">
          <span className="max-w-[120px] truncate">{uploadError}</span>
          <button type="button" onClick={onClearError} className="hover:text-red-300">
            <X size={12} />
          </button>
        </span>
      )}

      <input
        ref={fileInputRef}
        type="file"
        accept=".csv,.xlsx,.xls,.kmz,.kml,.json"
        onChange={handleFileSelect}
        className="hidden"
      />
    </div>
  );
});

export default FileUpload;
