import ReactMarkdown from 'react-markdown';

export default function MessageBubble({ message }) {
  const isUser = message.role === 'user';

  return (
    <div className={`flex ${isUser ? 'justify-end' : 'justify-start'}`}>
      <div
        className={`max-w-[75%] rounded-lg px-4 py-2 ${
          isUser
            ? 'bg-vantyra-bg-h text-vantyra-text'
            : message.responseType === 'error'
              ? 'bg-red-950/30 text-vantyra-error border border-red-800/50'
              : 'bg-vantyra-bg-s text-vantyra-text'
        }`}
      >
        {isUser ? (
          <p className="whitespace-pre-wrap">{message.content}</p>
        ) : (
          <div className="prose prose-sm prose-invert max-w-none prose-p:my-1 prose-ul:my-1 prose-ol:my-1 prose-pre:bg-[#1a1a2e] prose-pre:text-gray-100 prose-a:text-vantyra-accent prose-headings:text-vantyra-text prose-strong:text-vantyra-text prose-code:text-vantyra-accent">
            <ReactMarkdown>{message.content}</ReactMarkdown>
          </div>
        )}
      </div>
    </div>
  );
}
