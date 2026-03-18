const INDENT = "[ \\t\\u3000]*";

const MAIN_HEADING_RE = new RegExp(
  `^${INDENT}主${INDENT}文${INDENT}$`,
  "m"
);

const REASON_HEADING_RE = new RegExp(
  `^${INDENT}(?:事實${INDENT}及${INDENT}理由|犯罪事實${INDENT}及${INDENT}理由|理${INDENT}由)${INDENT}$`,
  "m"
);

function normalizeText(text) {
  return (text || "").replace(/\r\n/g, "\n").replace(/\r/g, "\n");
}

function findHeading(text, regex, startIndex = 0) {
  const match = regex.exec(text.slice(startIndex));
  if (!match) return null;

  const index = startIndex + match.index;
  let contentStart = index + match[0].length;
  while (contentStart < text.length && text[contentStart] === "\n") {
    contentStart += 1;
  }

  return { index, contentStart, heading: match[0] };
}

export function parseDecisionSections(text) {
  const normalized = normalizeText(text);
  const mainHeading = findHeading(normalized, MAIN_HEADING_RE);
  const reasonHeading = findHeading(
    normalized,
    REASON_HEADING_RE,
    mainHeading ? mainHeading.contentStart : 0
  );

  if (!mainHeading && !reasonHeading) {
    return { header: normalized.trim(), main: "", reason: "" };
  }

  if (!mainHeading && reasonHeading) {
    return {
      header: normalized.slice(0, reasonHeading.index).trim(),
      main: "",
      reason: normalized.slice(reasonHeading.contentStart).trim(),
    };
  }

  const header = normalized.slice(0, mainHeading.index).trim();
  const mainEnd = reasonHeading ? reasonHeading.index : normalized.length;
  const main = normalized.slice(mainHeading.contentStart, mainEnd).trim();
  const reason = reasonHeading
    ? normalized.slice(reasonHeading.contentStart).trim()
    : "";

  return { header, main, reason };
}
