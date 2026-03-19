const INDENT = "[ \\t\\u3000]*";

const MAIN_HEADING_RE = new RegExp(
  `^${INDENT}主${INDENT}文${INDENT}$`,
  "m"
);

// 獨立「事實」標題（刑事簡式審判）
const FACT_HEADING_RE = new RegExp(
  `^${INDENT}事${INDENT}實${INDENT}$`,
  "m"
);

// 「理由」類標題（含事實及理由、犯罪事實及理由等合併形式）
const REASON_HEADING_RE = new RegExp(
  `^${INDENT}(?:事實${INDENT}及${INDENT}理由|犯罪事實${INDENT}及${INDENT}理由|事實${INDENT}與${INDENT}理由|理${INDENT}由(?:${INDENT}要${INDENT}領)?)${INDENT}$`,
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
  const searchStart = mainHeading ? mainHeading.contentStart : 0;

  // 先找獨立「事實」標題
  const factHeading = findHeading(normalized, FACT_HEADING_RE, searchStart);

  // 再找「理由」類標題：若有獨立事實，從事實後找；否則從主文後找
  const reasonSearchStart = factHeading ? factHeading.contentStart : searchStart;
  const reasonHeading = findHeading(normalized, REASON_HEADING_RE, reasonSearchStart);

  if (!mainHeading && !factHeading && !reasonHeading) {
    return { header: normalized.trim(), main: "", fact: "", reason: "" };
  }

  const factLabel = factHeading
    ? factHeading.heading.replace(/[\u3000\s]/g, "")
    : "";
  const reasonLabel = reasonHeading
    ? reasonHeading.heading.replace(/[\u3000\s]/g, "")
    : "";

  if (!mainHeading) {
    const firstH = factHeading || reasonHeading;
    return {
      header: normalized.slice(0, firstH.index).trim(),
      main: "",
      fact: factHeading
        ? normalized
            .slice(factHeading.contentStart, reasonHeading ? reasonHeading.index : undefined)
            .trim()
        : "",
      reason: reasonHeading
        ? normalized.slice(reasonHeading.contentStart).trim()
        : normalized.slice(factHeading.contentStart).trim(),
      factLabel,
      reasonLabel,
    };
  }

  const mainEnd = factHeading
    ? factHeading.index
    : reasonHeading
    ? reasonHeading.index
    : normalized.length;

  return {
    header: normalized.slice(0, mainHeading.index).trim(),
    main: normalized.slice(mainHeading.contentStart, mainEnd).trim(),
    fact: factHeading
      ? normalized
          .slice(factHeading.contentStart, reasonHeading ? reasonHeading.index : normalized.length)
          .trim()
      : "",
    reason: reasonHeading ? normalized.slice(reasonHeading.contentStart).trim() : "",
    factLabel,
    reasonLabel,
  };
}
