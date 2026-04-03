import { useState, useEffect, useRef } from 'react';
import { useChat } from './hooks/useChat';
import { useFileUpload } from './hooks/useFileUpload';
import Header from './components/Header';
import ChatPanel from './components/ChatPanel';
import MessageInput from './components/MessageInput';
import ErrorBanner from './components/ErrorBanner';
import FileUpload from './components/FileUpload';

function App() {
  const { sessionId, messages, isLoading, pendingPlan, executionSteps, sendMessage, approvePlan, injectUploadMessage, resetChat } = useChat();
  const { isUploading, uploadError, lastUpload, uploadFile, clearUploadError } = useFileUpload();
  const [globalError, setGlobalError] = useState(null);
  const [isDragOver, setIsDragOver] = useState(false);
  const fileUploadRef = useRef(null);

  useEffect(() => {
    if (lastUpload) {
      injectUploadMessage(lastUpload.filename, lastUpload.path, lastUpload.file_type);
    }
  }, [lastUpload, injectUploadMessage]);

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
        onSend={sendMessage}
        disabled={isLoading}
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
