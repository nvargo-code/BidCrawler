import type { Bid } from "@/types/bid";
import { formatDate, formatValue } from "./utils";

function formatLocation(bid: Bid): string {
  const parts = [bid.location_city, bid.location_county ? `${bid.location_county} Co.` : "", bid.location_state]
    .filter(Boolean);
  return parts.length ? parts.join(", ") : "—";
}

function formatDueDate(bid: Bid): string {
  return bid.due_date ? formatDate(bid.due_date) : "No due date";
}

function formatEstimatedValue(bid: Bid): string {
  const v = formatValue(bid.estimated_value);
  return v || "Not listed";
}

function truncateDescription(bid: Bid, maxLen = 180): string {
  const d = (bid.description ?? "").trim();
  if (!d) return "—";
  return d.length > maxLen ? `${d.slice(0, maxLen).trim()}…` : d;
}

function escapeHtml(s: string): string {
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

export function buildShortlistEmailHtml(bids: Bid[]): string {
  const rows = bids
    .map((bid, i) => {
      const bg = i % 2 === 0 ? "#ffffff" : "#f7f7f8";
      const title = escapeHtml(bid.title || "Untitled");
      const agency = escapeHtml(bid.agency || "Unknown agency");
      const link = bid.bid_url
        ? `<a href="${escapeHtml(bid.bid_url)}" style="color:#2563eb;text-decoration:none;">View bid →</a>`
        : "—";
      return `
        <tr style="background:${bg};">
          <td style="padding:10px 12px;border:1px solid #e5e7eb;font-weight:600;color:#111827;">${title}<br/>
            <span style="font-weight:400;color:#6b7280;font-size:12px;">${agency}</span>
          </td>
          <td style="padding:10px 12px;border:1px solid #e5e7eb;color:#374151;">${escapeHtml(formatLocation(bid))}</td>
          <td style="padding:10px 12px;border:1px solid #e5e7eb;color:#374151;white-space:nowrap;">${escapeHtml(formatEstimatedValue(bid))}</td>
          <td style="padding:10px 12px;border:1px solid #e5e7eb;color:#374151;white-space:nowrap;">${escapeHtml(formatDueDate(bid))}</td>
          <td style="padding:10px 12px;border:1px solid #e5e7eb;color:#374151;font-size:13px;">${escapeHtml(truncateDescription(bid))}</td>
          <td style="padding:10px 12px;border:1px solid #e5e7eb;">${link}</td>
        </tr>`;
    })
    .join("");

  return `
  <div style="font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;color:#111827;">
    <h2 style="margin:0 0 4px;">Bid Shortlist</h2>
    <p style="margin:0 0 16px;color:#6b7280;">${bids.length} project${bids.length === 1 ? "" : "s"} for comparison</p>
    <table style="border-collapse:collapse;width:100%;font-size:14px;">
      <thead>
        <tr style="background:#111827;">
          <th style="padding:10px 12px;border:1px solid #e5e7eb;text-align:left;color:#ffffff;">Project / Agency</th>
          <th style="padding:10px 12px;border:1px solid #e5e7eb;text-align:left;color:#ffffff;">Location</th>
          <th style="padding:10px 12px;border:1px solid #e5e7eb;text-align:left;color:#ffffff;">Est. Value</th>
          <th style="padding:10px 12px;border:1px solid #e5e7eb;text-align:left;color:#ffffff;">Bid Due</th>
          <th style="padding:10px 12px;border:1px solid #e5e7eb;text-align:left;color:#ffffff;">Description</th>
          <th style="padding:10px 12px;border:1px solid #e5e7eb;text-align:left;color:#ffffff;">Link</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>
  </div>`;
}

export function buildShortlistEmailText(bids: Bid[]): string {
  return bids
    .map((bid, i) => {
      return `${i + 1}. ${bid.title ?? "Untitled"}
   Agency: ${bid.agency ?? "Unknown"}
   Location: ${formatLocation(bid)}
   Est. Value: ${formatEstimatedValue(bid)}
   Bid Due: ${formatDueDate(bid)}
   Description: ${truncateDescription(bid)}
   ${bid.bid_url ?? "No URL"}`;
    })
    .join("\n\n");
}
