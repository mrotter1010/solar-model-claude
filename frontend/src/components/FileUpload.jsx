import { useState, useRef } from 'react';
import { Upload, X, CheckCircle } from 'lucide-react';

const FILE_TYPES = [
  { value: 'rate', label: 'Rate Schedule' },
  { value: 'kmz', label: 'KMZ Boundary' },
  { value: 'load-profile', label: 'Load Profile' },
];

function FileUpload({ onUpload, isUploading, uploadError, lastUpload, onClearError }) {
  const [fileType, setFileType] = useState('rate');
  const [selectedFile, setSelectedFile] = useState(null);
  const [isDragOver, setIsDragOver] = useState(false);
  const fileInputRef = useRef(null);

  const handleDrop = (e) => {
    e.preventDefault();
    setIsDragOver(false);
    const file = e.dataTransfer.files[0];
    if (file) setSelectedFile(file);
  };

  const handleDragOver = (e) => {
    e.preventDefault();
    setIsDragOver(true);
  };

  const handleDragLeave = () => {
    setIsDragOver(false);
  };

  const handleFileSelect = (e) => {
    const file = e.target.files[0];
    if (file) setSelectedFile(file);
  };

  const handleUpload = () => {
    if (selectedFile && !isUploading) {
      onUpload(selectedFile, fileType);
    }
  };

  return (
    <div className="space-y-3">
      {/* File type selector */}
      <div>
        <label className="block text-sm font-medium text-gray-700 mb-1">File Type</label>
        <select
          value={fileType}
          onChange={(e) => setFileType(e.target.value)}
          className="w-full rounded-md border border-gray-300 bg-white px-3 py-2 text-sm shadow-sm focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
        >
          {FILE_TYPES.map((ft) => (
            <option key={ft.value} value={ft.value}>{ft.label}</option>
          ))}
        </select>
      </div>

      {/* Drop zone */}
      <div
        onDrop={handleDrop}
        onDragOver={handleDragOver}
        onDragLeave={handleDragLeave}
        onClick={() => fileInputRef.current?.click()}
        className={`flex flex-col items-center justify-center rounded-md border-2 border-dashed p-4 cursor-pointer transition-colors ${
          isDragOver
            ? 'border-blue-400 bg-blue-50'
            : 'border-gray-300 bg-gray-50 hover:border-blue-400 hover:bg-blue-50/50'
        }`}
      >
        <Upload className="h-6 w-6 text-gray-400 mb-2" />
        {selectedFile ? (
          <p className="text-sm text-gray-700 text-center truncate max-w-full">{selectedFile.name}</p>
        ) : (
          <p className="text-sm text-gray-500 text-center">Drop file here or click to browse</p>
        )}
        <input
          ref={fileInputRef}
          type="file"
          onChange={handleFileSelect}
          className="hidden"
        />
      </div>

      {/* Upload button */}
      <button
        onClick={handleUpload}
        disabled={!selectedFile || isUploading}
        className="w-full rounded-md bg-blue-600 px-3 py-2 text-sm font-medium text-white shadow-sm hover:bg-blue-700 disabled:bg-gray-300 disabled:cursor-not-allowed transition-colors"
      >
        {isUploading ? 'Uploading...' : 'Upload'}
      </button>

      {/* Status area */}
      {lastUpload && !uploadError && (
        <div className="flex items-center gap-1.5 text-sm text-green-600">
          <CheckCircle className="h-4 w-4 shrink-0" />
          <span className="truncate">✓ {lastUpload.filename} uploaded successfully</span>
        </div>
      )}
      {uploadError && (
        <div className="flex items-center justify-between gap-2 text-sm text-red-600">
          <span className="truncate">{uploadError}</span>
          <button onClick={onClearError} className="shrink-0 p-0.5 hover:text-red-800">
            <X className="h-4 w-4" />
          </button>
        </div>
      )}
    </div>
  );
}

export default FileUpload;
