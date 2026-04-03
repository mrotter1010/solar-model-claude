import { useState, useRef, useEffect, useCallback, forwardRef, useImperativeHandle } from 'react';
import { Paperclip, X, CheckCircle, Loader2 } from 'lucide-react';

const FILE_TYPES = [
  { value: 'rate', label: 'Rate Schedule' },
  { value: 'kmz', label: 'KMZ Boundary' },
  { value: 'load-profile', label: 'Load Profile' },
];

const FileUpload = forwardRef(function FileUpload(
  { onUpload, isUploading, uploadError, lastUpload, onClearError },
  ref
) {
  const [fileType, setFileType] = useState('rate');
  const [showPopover, setShowPopover] = useState(false);
  const [popoverStyle, setPopoverStyle] = useState({});
  const fileInputRef = useRef(null);
  const popoverRef = useRef(null);
  const anchorRef = useRef(null);

  // Expose drop handler so parent can forward drag-and-drop events
  useImperativeHandle(ref, () => ({
    handleFileDrop: (file) => {
      if (!isUploading) {
        onUpload(file, fileType);
      }
    },
  }));

  // Compute fixed position for popover so it escapes ancestor stacking contexts
  const updatePopoverPosition = useCallback(() => {
    if (!anchorRef.current) return;
    const rect = anchorRef.current.getBoundingClientRect();
    setPopoverStyle({
      position: 'fixed',
      bottom: window.innerHeight - rect.top + 18,
      left: rect.left,
    });
  }, []);

  // Close popover on outside click
  useEffect(() => {
    if (!showPopover) return;
    const handleClick = (e) => {
      if (
        popoverRef.current && !popoverRef.current.contains(e.target) &&
        anchorRef.current && !anchorRef.current.contains(e.target)
      ) {
        setShowPopover(false);
      }
    };
    document.addEventListener('mousedown', handleClick);
    return () => document.removeEventListener('mousedown', handleClick);
  }, [showPopover]);

  // Recalculate position when popover opens or window resizes
  useEffect(() => {
    if (!showPopover) return;
    updatePopoverPosition();
    window.addEventListener('resize', updatePopoverPosition);
    return () => window.removeEventListener('resize', updatePopoverPosition);
  }, [showPopover, updatePopoverPosition]);

  const handleFileSelect = (e) => {
    const file = e.target.files[0];
    if (file && !isUploading) {
      onUpload(file, fileType);
      setShowPopover(false);
    }
    // Reset so the same file can be re-selected
    e.target.value = '';
  };

  return (
    <div className="relative flex items-center gap-1.5" ref={anchorRef}>
      <button
        type="button"
        onClick={() => setShowPopover(!showPopover)}
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

      {/* Upload success chip */}
      {lastUpload && !uploadError && !isUploading && (
        <span className="inline-flex items-center gap-1 rounded-full bg-emerald-950/40 border border-emerald-800/50 px-2 py-0.5 text-xs text-vantyra-success">
          <CheckCircle size={12} />
          <span className="max-w-[120px] truncate">{lastUpload.filename}</span>
        </span>
      )}

      {/* Upload error chip */}
      {uploadError && (
        <span className="inline-flex items-center gap-1 rounded-full bg-red-950/40 border border-red-800/50 px-2 py-0.5 text-xs text-vantyra-error">
          <span className="max-w-[120px] truncate">{uploadError}</span>
          <button type="button" onClick={onClearError} className="hover:text-red-300">
            <X size={12} />
          </button>
        </span>
      )}

      {/* File type + browse popover */}
      {showPopover && (
        <div
          ref={popoverRef}
          className="w-56 rounded-lg border border-vantyra-border bg-vantyra-bg-s p-3 shadow-2xl space-y-3 z-50"
          style={popoverStyle}
        >
          <div>
            <label className="block text-xs font-medium text-vantyra-text-s mb-1">File Type</label>
            <select
              value={fileType}
              onChange={(e) => setFileType(e.target.value)}
              className="w-full rounded-md border border-vantyra-border bg-vantyra-bg-h px-2 py-1.5 text-sm text-vantyra-text focus:border-vantyra-accent focus:outline-none focus:ring-1 focus:ring-vantyra-accent"
              style={{ backgroundColor: '#2d2d50', color: '#e8e8f0' }}
            >
              {FILE_TYPES.map((ft) => (
                <option key={ft.value} value={ft.value} style={{ backgroundColor: '#252540', color: '#e8e8f0' }}>{ft.label}</option>
              ))}
            </select>
          </div>
          <button
            type="button"
            onClick={() => fileInputRef.current?.click()}
            className="w-full rounded-xl border-2 border-cyan-300 bg-cyan-500 px-5 py-2.5 text-sm font-bold text-white shadow-[0_0_24px_rgba(0,212,255,0.5)] hover:brightness-110 transition-all"
          >
            Browse files…
          </button>
        </div>
      )}

      <input
        ref={fileInputRef}
        type="file"
        onChange={handleFileSelect}
        className="hidden"
      />
    </div>
  );
});

export default FileUpload;
