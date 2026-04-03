import { useEffect, useRef } from 'react';
import vantyraIcon from '../assets/vantyra-icon.png';
import MessageBubble from './MessageBubble';
import PlanCard from './PlanCard';
import ResultsCard from './ResultsCard';
import LoadingState from './LoadingState';
import ExecutionProgress from './ExecutionProgress';
import { CONFIG } from '../config.js';

export default function ChatPanel({ messages, isLoading, pendingPlan, executionSteps = [], onApprove }) {
  const bottomRef = useRef(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages, isLoading, executionSteps]);

  // Find the index of the last plan message to decide where to show the button
  let lastPlanIndex = -1;
  for (let i = messages.length - 1; i >= 0; i--) {
    if (messages[i].responseType === 'plan') {
      lastPlanIndex = i;
      break;
    }
  }

  if (messages.length === 0 && !isLoading) {
    return (
      <div className="flex-1 flex flex-col items-center justify-center text-vantyra-text-s">
        <img src={vantyraIcon} alt="Vantyra" className="h-24 w-24 object-contain mb-3" />
        <p className="text-sm">Describe the analysis you want to get started</p>
      </div>
    );
  }

  return (
    <div className="flex-1 overflow-y-auto p-4 space-y-4">
      {messages.map((msg, idx) => {
        if (msg.responseType === 'plan') {
          const isLastPlan = idx === lastPlanIndex && pendingPlan;
          return (
            <PlanCard
              key={msg.id}
              content={msg.content}
              onApprove={isLastPlan ? onApprove : null}
              isLoading={isLoading}
            />
          );
        }
        if (msg.steps != null) {
          return (
            <ResultsCard
              key={msg.id}
              content={msg.content}
              steps={msg.steps}
              analysisApiUrl={CONFIG.analysisApiUrl}
            />
          );
        }
        return <MessageBubble key={msg.id} message={msg} />;
      })}
      {isLoading && executionSteps.length > 0 && (
        <ExecutionProgress steps={executionSteps} />
      )}
      {isLoading && executionSteps.length === 0 && <LoadingState />}
      <div ref={bottomRef} />
    </div>
  );
}
