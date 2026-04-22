import ReactMarkdown from 'react-markdown';
import rehypeRaw from 'rehype-raw';
import { Download, CheckCircle, XCircle } from 'lucide-react';
import { markdownComponents } from './MarkdownRenderers';
import { CONFIG } from '../config.js';
import StepsList from './StepsList';

const RUN_TOOLS = ['run_production', 'run_bill_savings', 'run_bess', 'run_buildability', 'run_optimization'];
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

function buildBatchDownloadUrl(baseUrl, workbookPath) {
  const url = `${baseUrl}${workbookPath}`;
  if (CONFIG.analysisApiKey) {
    return `${url}?api_key=${CONFIG.analysisApiKey}`;
  }
  return url;
}

function formatRuntime(seconds) {
  if (seconds < 60) return `${Math.round(seconds)}s`;
  const m = Math.floor(seconds / 60);
  const s = Math.round(seconds % 60);
  return s > 0 ? `${m}m ${s}s` : `${m}m`;
}

export default function ResultsCard({ content, steps, analysisApiUrl }) {
  const downloadableSteps = steps.filter(s =>
    s.success && RUN_TOOLS.includes(s.tool) && s.result?.run_id
  );

  const batchSteps = steps.filter(s =>
    s.success && s.tool === 'run_batch' && s.result?.files?.workbook_url
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
        {batchSteps.length > 0 && (
          <div className="mt-3 border-t border-vantyra-border pt-3 space-y-3">
            {batchSteps.map((step, idx) => {
              const r = step.result;
              const failedRows = r.summary?.filter(s => s.status !== 'success') || [];
              return (
                <div key={`batch-${idx}`} className="text-sm space-y-2">
                  <p className="text-xs font-medium text-vantyra-text-s">Batch Processing</p>
                  <p className="text-vantyra-text">
                    Batch complete:{' '}
                    <span className="font-medium text-vantyra-success">
                      {r.succeeded}/{r.total_rows} succeeded
                    </span>
                    {r.failed > 0 && (
                      <span className="font-medium text-vantyra-error">, {r.failed} failed</span>
                    )}
                  </p>
                  <p className="text-xs text-vantyra-text-s">
                    Total runtime: {formatRuntime(r.total_runtime_seconds)}
                  </p>
                  {failedRows.length > 0 && (
                    <div className="rounded-md bg-red-950/30 border border-red-800/40 px-3 py-2 space-y-1">
                      <p className="text-xs font-medium text-vantyra-error">Failed rows:</p>
                      {failedRows.map((row, i) => (
                        <div key={i} className="flex items-start gap-1.5 text-xs text-vantyra-error">
                          <XCircle size={12} className="shrink-0 mt-0.5" />
                          <span>{row.name}: {row.error || 'Unknown error'}</span>
                        </div>
                      ))}
                    </div>
                  )}
                  {r.warnings?.length > 0 && (
                    <p className="text-xs text-yellow-400">
                      {r.warnings.length} warning{r.warnings.length !== 1 ? 's' : ''} during validation
                    </p>
                  )}
                  <a
                    href={buildBatchDownloadUrl(analysisApiUrl, r.files.workbook_url)}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="inline-flex items-center gap-1 rounded-xl border-2 border-cyan-300 bg-cyan-500 px-5 py-2.5 text-sm font-bold text-white shadow-[0_0_24px_rgba(0,212,255,0.5)] hover:brightness-110"
                  >
                    <Download size={12} />
                    Download Batch Results
                  </a>
                </div>
              );
            })}
          </div>
        )}
        <StepsList steps={steps} />
      </div>
    </div>
  );
}
