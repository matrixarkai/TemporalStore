const fs = require("fs");
const path = require("path");
const { marked } = require("marked");
const { chromium } = require("playwright");

function decodeHtml(value) {
  return value
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'");
}

async function main() {
  const input = process.argv[2];
  const outputPdf = process.argv[3];
  const outputHtml = process.argv[4] || outputPdf.replace(/\.pdf$/i, ".html");

  if (!input || !outputPdf) {
    console.error("usage: node render_markdown_mermaid_pdf.cjs input.md output.pdf [output.html]");
    process.exit(2);
  }

  const md = fs.readFileSync(input, "utf8");
  let body = marked.parse(md, { mangle: false, headerIds: true });
  body = body.replace(
    /<pre><code class="language-mermaid">([\s\S]*?)<\/code><\/pre>/g,
    (_, code) => `<div class="mermaid">${decodeHtml(code).trim()}</div>`
  );

  const html = `<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>TemporalStore</title>
  <style>
    @page { size: Letter; margin: 0.55in; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Arial, sans-serif;
      color: #1f2933;
      line-height: 1.48;
      font-size: 10.5pt;
    }
    h1, h2, h3 { color: #111827; page-break-after: avoid; }
    h1 { font-size: 25pt; margin: 0 0 18px; }
    h2 { font-size: 17pt; margin: 28px 0 10px; border-top: 1px solid #d9e2ec; padding-top: 16px; }
    h3 { font-size: 13pt; margin: 20px 0 8px; }
    p { margin: 7px 0; }
    pre {
      background: #f5f7fa;
      border: 1px solid #d9e2ec;
      border-radius: 6px;
      padding: 10px 12px;
      white-space: pre-wrap;
      overflow-wrap: anywhere;
      font-size: 9pt;
      page-break-inside: avoid;
    }
    code { font-family: "Cascadia Mono", Consolas, monospace; }
    table { width: 100%; border-collapse: collapse; margin: 12px 0; page-break-inside: avoid; }
    th, td { border: 1px solid #d9e2ec; padding: 7px; vertical-align: top; }
    th { background: #eef2f7; }
    li { margin: 3px 0; }
    .mermaid {
      background: #ffffff;
      border: 1px solid #d9e2ec;
      border-radius: 8px;
      padding: 12px;
      margin: 12px 0 18px;
      page-break-inside: avoid;
      text-align: center;
    }
    .mermaid svg {
      max-width: 100%;
      height: auto !important;
    }
  </style>
</head>
<body>
${body}
<script src="https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.min.js"></script>
<script>
  mermaid.initialize({
    startOnLoad: false,
    securityLevel: "loose",
    theme: "base",
    themeVariables: {
      fontFamily: "-apple-system, BlinkMacSystemFont, Segoe UI, Arial, sans-serif",
      primaryColor: "#eef6ff",
      primaryBorderColor: "#3b82f6",
      primaryTextColor: "#111827",
      lineColor: "#475569",
      secondaryColor: "#f0fdf4",
      tertiaryColor: "#fff7ed"
    }
  });
</script>
</body>
</html>`;

  fs.mkdirSync(path.dirname(outputPdf), { recursive: true });
  fs.writeFileSync(outputHtml, html);

  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({ viewport: { width: 1200, height: 1600 } });
  await page.goto("file://" + path.resolve(outputHtml).replace(/\\/g, "/"), { waitUntil: "networkidle" });
  await page.waitForFunction(() => window.mermaid !== undefined, null, { timeout: 60000 });
  await page.evaluate(async () => {
    await window.mermaid.run({ querySelector: ".mermaid" });
  });
  await page.waitForTimeout(500);
  await page.pdf({
    path: outputPdf,
    format: "Letter",
    printBackground: true,
    margin: { top: "0.55in", right: "0.55in", bottom: "0.55in", left: "0.55in" }
  });
  await browser.close();

  console.log(`wrote ${outputPdf}`);
  console.log(`wrote ${outputHtml}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
