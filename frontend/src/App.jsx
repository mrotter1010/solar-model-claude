import { useState, useCallback, useRef, useEffect } from 'react';
import { useChat } from './hooks/useChat';
import { useFileUpload } from './hooks/useFileUpload';
import { getConversations, deleteConversation, renameConversation } from './api/orchestrator.js';
import Header from './components/Header';
import Sidebar from './components/Sidebar';
import ChatPanel from './components/ChatPanel';
import MessageInput from './components/MessageInput';
import ErrorBanner from './components/ErrorBanner';
import FileUpload from './components/FileUpload';
import AccessGate from './components/AccessGate';
import { getInviteCode, setInviteCode } from './utils/inviteCode.js';
import { getOrCreateUserId } from './utils/userIdentity.js';

function buildFileContext(upload) {
  if (upload.extracted_text) {
    return `[System: File uploaded — ${upload.file_type} file '${upload.filename}' available at server path: ${upload.path}. File contents:\n${upload.extracted_text}]`;
  }
  if (upload.file_type !== 'unknown') {
    return `[System: File uploaded — ${upload.file_type} file '${upload.filename}' available at server path: ${upload.path}]`;
  }
  return `[System: File uploaded — '${upload.filename}' available at server path: ${upload.path}. Binary file — contents not readable.]`;
}

function App() {
  // Ensure anonymous user identity cookie exists before any API calls.
  useState(() => getOrCreateUserId());
  const [hasAccess, setHasAccess] = useState(() => !!getInviteCode());
  const { conversationId, messages, isLoading, pendingPlan, executionSteps, sendMessage, approvePlan, loadConversation, resetChat } = useChat();
  const { isUploading, uploadError, lastUpload, uploadFile, clearUploadError, clearLastUpload } = useFileUpload();
  const [conversations, setConversations] = useState([]);
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [globalError, setGlobalError] = useState(null);
  const [isDragOver, setIsDragOver] = useState(false);
  const fileUploadRef = useRef(null);

  const handleAccessGranted = useCallback((code) => {
    setInviteCode(code);
    setHasAccess(true);
  }, []);

  const fetchConversations = useCallback(async () => {
    try {
      const list = await getConversations();
      setConversations(list);
    } catch (err) {
      console.error('Failed to fetch conversations:', err);
    }
  }, []);

  useEffect(() => {
    if (hasAccess) fetchConversations();
  }, [hasAccess, fetchConversations]);

  if (!hasAccess) {
    return <AccessGate onSuccess={handleAccessGranted} />;
  }

  const handleDeleteConversation = useCallback(async (id) => {
    try {
      const deleted = await deleteConversation(id);
      if (deleted) {
        if (id === conversationId) resetChat();
        fetchConversations();
      }
    } catch (err) {
      console.error('Failed to delete conversation:', err);
    }
  }, [conversationId, resetChat, fetchConversations]);

  const handleRenameConversation = useCallback(async (id, title) => {
    await renameConversation(id, title);
    fetchConversations();
  }, [fetchConversations]);

  const handleSend = useCallback(async (text) => {
    if (lastUpload) {
      const fileContext = buildFileContext(lastUpload);
      const fileAttachment = { filename: lastUpload.filename, file_type: lastUpload.file_type };
      clearLastUpload();
      await sendMessage(text, fileContext, fileAttachment);
    } else {
      await sendMessage(text);
    }
    fetchConversations();
  }, [lastUpload, clearLastUpload, sendMessage, fetchConversations]);

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
      className={`h-screen flex bg-vantyra-bg ${isDragOver ? 'ring-2 ring-inset ring-vantyra-accent' : ''}`}
      onDragOver={handleDragOver}
      onDragLeave={handleDragLeave}
      onDrop={handleDrop}
    >
      <Sidebar
        isOpen={sidebarOpen}
        onToggle={() => setSidebarOpen(prev => !prev)}
        conversations={conversations}
        activeConversationId={conversationId}
        onSelectConversation={loadConversation}
        onDeleteConversation={handleDeleteConversation}
        onRenameConversation={handleRenameConversation}
      />

      <div className="flex-1 flex flex-col min-w-0">
        <Header onNewChat={() => { resetChat(); fetchConversations(); }} />
        {globalError && (
          <ErrorBanner message={globalError} onDismiss={() => setGlobalError(null)} />
        )}
        <ChatPanel messages={messages} isLoading={isLoading} pendingPlan={pendingPlan} executionSteps={executionSteps} onApprove={async () => { await approvePlan(); fetchConversations(); }} />
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
    </div>
  );
}

export default App
