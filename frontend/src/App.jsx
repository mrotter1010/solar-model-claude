import { useState, useEffect } from 'react';
import { useChat } from './hooks/useChat';
import { useFileUpload } from './hooks/useFileUpload';
import ChatPanel from './components/ChatPanel';
import MessageInput from './components/MessageInput';
import ErrorBanner from './components/ErrorBanner';
import FileUpload from './components/FileUpload';

function App() {
  const { sessionId, messages, isLoading, pendingPlan, executionSteps, sendMessage, approvePlan, injectUploadMessage, resetChat } = useChat();
  const { isUploading, uploadError, lastUpload, uploadFile, clearUploadError } = useFileUpload();
  const [globalError, setGlobalError] = useState(null);

  useEffect(() => {
    if (lastUpload) {
      injectUploadMessage(lastUpload.filename, lastUpload.path, lastUpload.file_type);
    }
  }, [lastUpload, injectUploadMessage]);

  return (
    <div className="h-screen flex bg-gray-50">
      {/* Left sidebar */}
      <aside className="w-80 shrink-0 flex flex-col border-r border-gray-200 bg-white">
        <div className="border-b border-gray-200 px-4 py-3 flex items-start justify-between">
          <div>
            <h1 className="text-lg font-semibold text-gray-900">Solar Analysis</h1>
            <p className="text-xs text-gray-400 font-mono">{sessionId.slice(0, 8)}</p>
          </div>
          <button
            onClick={resetChat}
            className="mt-0.5 rounded px-2 py-1 text-xs font-medium text-gray-500 hover:bg-gray-100 hover:text-gray-700"
          >
            + New Chat
          </button>
        </div>
        <div className="p-4">
          <FileUpload
            onUpload={uploadFile}
            isUploading={isUploading}
            uploadError={uploadError}
            lastUpload={lastUpload}
            onClearError={clearUploadError}
          />
        </div>
      </aside>

      {/* Right main area */}
      <main className="flex-1 flex flex-col min-w-0">
        {globalError && (
          <ErrorBanner message={globalError} onDismiss={() => setGlobalError(null)} />
        )}
        <ChatPanel messages={messages} isLoading={isLoading} pendingPlan={pendingPlan} executionSteps={executionSteps} onApprove={approvePlan} />
        <MessageInput onSend={sendMessage} disabled={isLoading} />
      </main>
    </div>
  );
}

export default App
