import ReactMarkdown from 'react-markdown';
import rehypeRaw from 'rehype-raw';
import { Download } from 'lucide-react';
import { markdownComponents } from './MarkdownRenderers';
import { CONFIG } from '../config.js';
import StepsList from './StepsList';

const RUN_TOOLS = ['run_production', 'run_bill_savings', 'run_bess', 'run_buildability'];
const TOOLS_WITH_TIMESERIES = ['run_production', 'run_bill_savings', 'run_bess'];

function formatToolName(name) {
  return name
    .replace(/_/g, ' ')
    .replace(/\b\w/g, c => c.toUpperCase());
}

function buildDownloadUrl(baseUrl, runId, type) {
  const suffix = type === 'pdf' ? 'report' : 'timeseries';
  const url = `${baseUrl}/analyses/${runId}/${suffix}`;
  if (CONFIG.analysisApiKey) {
    return `${url}?api_key=${CONFIG.analysisApiKey}`;
  }
  return url;
}

export default function ResultsCard({ content, steps, analysisApiUrl }) {
  const downloadableSteps = steps.filter(s =>
    s.success && RUN_TOOLS.includes(s.tool) && s.result?.run_id
  );

  return (
    <div className="flex justify-start">
      <div className="max-w-[75%] rounded-lg border border-vantyra-border bg-vantyra-bg-s px-4 py-3">
        <p className="text-xs font-semibold uppercase tracking-wide text-vantyra-success mb-2">
          Analysis Results
        </p>
        <div className="prose prose-sm prose-invert max-w-none prose-p:my-1 prose-ul:my-1 prose-ol:my-1 prose-pre:bg-[#1a1a2e] prose-pre:text-gray-100 prose-a:text-vantyra-accent prose-headings:text-vantyra-text prose-strong:text-vantyra-text prose-code:text-vantyra-accent">
          <ReactMarkdown components={markdownComponents} rehypePlugins={[rehypeRaw]}>{content}</ReactMarkdown>
        </div>
        {downloadableSteps.length > 0 && (
          <div className="mt-3 border-t border-vantyra-border pt-3 space-y-2">
            {downloadableSteps.map((step, idx) => (
              <div key={idx} className="text-sm">
                <p className="text-xs font-medium text-vantyra-text-s mb-1">
                  {formatToolName(step.tool)}
                </p>
                <div className="flex gap-2">
                  <a
                    href={buildDownloadUrl(analysisApiUrl, step.result.run_id, 'pdf')}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="inline-flex items-center gap-1 rounded-xl border-2 border-cyan-300 bg-cyan-500 px-5 py-2.5 text-sm font-bold text-white shadow-[0_0_24px_rgba(0,212,255,0.5)] hover:brightness-110"
                  >
                    <Download size={12} />
                    Download PDF
                  </a>
                  {TOOLS_WITH_TIMESERIES.includes(step.tool) && (
                    <a
                      href={buildDownloadUrl(analysisApiUrl, step.result.run_id, 'csv')}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="inline-flex items-center gap-1 rounded-xl border-2 border-cyan-300 px-5 py-2.5 text-sm font-bold text-cyan-300 hover:bg-vantyra-accent hover:text-white transition-colors"
                    >
                      <Download size={12} />
                      Download CSV
                    </a>
                  )}
                </div>
              </div>
            ))}
          </div>
        )}
        <StepsList steps={steps} />
      </div>
    </div>
  );
}
