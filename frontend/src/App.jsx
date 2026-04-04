import { useState, useCallback, useRef } from 'react';
import { useChat } from './hooks/useChat';
import { useFileUpload } from './hooks/useFileUpload';
import Header from './components/Header';
import ChatPanel from './components/ChatPanel';
import MessageInput from './components/MessageInput';
import ErrorBanner from './components/ErrorBanner';
import FileUpload from './components/FileUpload';

function buildFileContext(upload) {
  if (upload.file_type !== 'unknown') {
    return `[System: File uploaded — ${upload.file_type} file '${upload.filename}' available at server path: ${upload.path}]`;
  }
  if (upload.extracted_text) {
    return `[System: File uploaded — '${upload.filename}' available at server path: ${upload.path}. File contents:\n${upload.extracted_text}]`;
  }
  return `[System: File uploaded — '${upload.filename}' available at server path: ${upload.path}. Binary file — contents not readable.]`;
}

function App() {
  const { sessionId, messages, isLoading, pendingPlan, executionSteps, sendMessage, approvePlan, resetChat } = useChat();
  const { isUploading, uploadError, lastUpload, uploadFile, clearUploadError, clearLastUpload } = useFileUpload();
  const [globalError, setGlobalError] = useState(null);
  const [isDragOver, setIsDragOver] = useState(false);
  const fileUploadRef = useRef(null);

  const handleSend = useCallback((text) => {
    if (lastUpload) {
      const fileContext = buildFileContext(lastUpload);
      const fileAttachment = { filename: lastUpload.filename, file_type: lastUpload.file_type };
      clearLastUpload();
      sendMessage(text, fileContext, fileAttachment);
    } else {
      sendMessage(text);
    }
  }, [lastUpload, clearLastUpload, sendMessage]);

  const handleDragOver = (e) => {
    e.preventDefault();
    setIsDragOver(true);
  };

  const handleDragLeave = (e) => {
    // Only clear when leaving the container, not when entering a child
    if (!e.currentTarget.contains(e.relatedTarget)) {
      setIsDragOver(false);
    }
  };

  const handleDrop = (e) => {
    e.preventDefault();
    setIsDragOver(false);
    const file = e.dataTransfer.files[0];
    if (file) {
      fileUploadRef.current?.handleFileDrop(file);
    }
  };

  return (
    <div
      className={`h-screen flex flex-col bg-vantyra-bg ${isDragOver ? 'ring-2 ring-inset ring-vantyra-accent' : ''}`}
      onDragOver={handleDragOver}
      onDragLeave={handleDragLeave}
      onDrop={handleDrop}
    >
      <Header onNewChat={resetChat} />
      {globalError && (
        <ErrorBanner message={globalError} onDismiss={() => setGlobalError(null)} />
      )}
      <ChatPanel messages={messages} isLoading={isLoading} pendingPlan={pendingPlan} executionSteps={executionSteps} onApprove={approvePlan} />
      <MessageInput
        onSend={handleSend}
        disabled={isLoading}
        pendingUpload={lastUpload}
        onClearPendingUpload={clearLastUpload}
        leftSlot={
          <FileUpload
            ref={fileUploadRef}
            onUpload={uploadFile}
            isUploading={isUploading}
            uploadError={uploadError}
            lastUpload={lastUpload}
            onClearError={clearUploadError}
          />
        }
      />
    </div>
  );
}

export default App
