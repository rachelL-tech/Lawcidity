import { useState, useRef, useEffect } from "react";

const COURT_LEVEL_OPTIONS = [
  { value: 0, label: "憲法法庭" },
  { value: 1, label: "最高法院 / 最高行政法院" },
  { value: 2, label: "高等法院 / 高等行政法院" },
  { value: 3, label: "地方法院" },
  { value: 4, label: "簡易庭" },
];

const DOC_TYPE_OPTIONS = [
  { value: "判決", label: "判決" },
  { value: "裁定", label: "裁定" },
  { value: "判例", label: "判例" },
  { value: "決議", label: "決議" },
  { value: "釋字", label: "釋字" },
  { value: "憲判字", label: "憲判字" },
];

// 多選下拉篩選元件
function FilterDropdown({ label, options, selected, onChange }) {
  const [open, setOpen] = useState(false);
  const ref = useRef(null);

  // 點擊外面時關閉
  useEffect(() => {
    function handleClickOutside(e) {
      if (ref.current && !ref.current.contains(e.target)) setOpen(false);
    }
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  const hasSelection = selected.length > 0;
  const selectedLabels = options
    .filter((o) => selected.includes(o.value))
    .map((o) => o.label)
    .join("、");

  function toggle(value) {
    if (selected.includes(value)) {
      onChange(selected.filter((v) => v !== value));
    } else {
      onChange([...selected, value]);
    }
  }

  function clear(e) {
    e.stopPropagation();
    onChange([]);
    setOpen(false);
  }

  return (
    <div ref={ref} className="relative">
      <button
        onClick={() => setOpen(!open)}
        className={`inline-flex items-center gap-1 px-3 py-1 rounded-full border text-sm transition-colors ${
          hasSelection
            ? "border-brand text-brand bg-brand-light"
            : "border-brand-border text-gray-600 hover:border-brand"
        }`}
      >
        {hasSelection ? `${label}：${selectedLabels}` : label}
        {hasSelection && (
          <span
            onClick={clear}
            className="ml-1 text-brand/60 hover:text-brand font-medium"
          >
            ×
          </span>
        )}
      </button>

      {open && (
        <div className="absolute top-full left-0 mt-1 bg-white border border-brand-border rounded-lg shadow-lg p-2 z-10 min-w-48">
          {options.map((opt) => (
            <label
              key={opt.value}
              className="flex items-center gap-2 px-2 py-1.5 rounded hover:bg-brand-light/50 cursor-pointer text-sm"
            >
              <input
                type="checkbox"
                checked={selected.includes(opt.value)}
                onChange={() => toggle(opt.value)}
                className="accent-brand"
              />
              {opt.label}
            </label>
          ))}
        </div>
      )}
    </div>
  );
}

// target 篩選列：文書類型 + 法院層級
export default function TargetFilterBar({ req, onChange }) {
  function handleDocTypes(newDocTypes) {
    onChange({ ...req, doc_types: newDocTypes, page: 1 });
  }

  function handleCourtLevels(newLevels) {
    onChange({ ...req, court_levels: newLevels, page: 1 });
  }

  return (
    <div className="flex items-center gap-2 flex-wrap mb-3">
      <FilterDropdown
        label="文書類型"
        options={DOC_TYPE_OPTIONS}
        selected={req.doc_types}
        onChange={handleDocTypes}
      />
      <FilterDropdown
        label="法院層級"
        options={COURT_LEVEL_OPTIONS}
        selected={req.court_levels}
        onChange={handleCourtLevels}
      />
    </div>
  );
}
