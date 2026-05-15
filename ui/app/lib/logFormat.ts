const tsFormatter = new Intl.DateTimeFormat(undefined, {
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  hour12: false
});

const timeFormatter = new Intl.DateTimeFormat(undefined, {
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  hour12: false
});

const dateFormatter = new Intl.DateTimeFormat(undefined, {
  month: "short",
  day: "2-digit"
});

function logLevelPill(level: string) {
  const normalized = level.toLowerCase();
  if (normalized === "error" || normalized === "fatal") {
    return "bg-red-50 text-red-600 border border-red-100";
  }
  if (normalized === "warn" || normalized === "warning") {
    return "bg-amber-50 text-amber-600 border border-amber-100";
  }
  if (normalized === "debug" || normalized === "trace") {
    return "bg-gray-50 text-gray-400 border border-gray-100";
  }
  return "bg-blue-50 text-blue-600 border border-blue-100";
}

function logLevelColors(level: string) {
  const normalized = level.toLowerCase();
  if (normalized === "error" || normalized === "fatal") {
    return {
      dot: "bg-red-500",
      text: "text-red-700",
      pillActive: "bg-red-50 text-red-700 border-red-200",
      pillHover: "hover:bg-red-50 hover:text-red-700 hover:border-red-200"
    };
  }
  if (normalized === "warn" || normalized === "warning") {
    return {
      dot: "bg-amber-500",
      text: "text-amber-700",
      pillActive: "bg-amber-50 text-amber-700 border-amber-200",
      pillHover: "hover:bg-amber-50 hover:text-amber-700 hover:border-amber-200"
    };
  }
  if (normalized === "debug" || normalized === "trace") {
    return {
      dot: "bg-gray-400",
      text: "text-gray-500",
      pillActive: "bg-gray-100 text-gray-700 border-gray-200",
      pillHover: "hover:bg-gray-100 hover:text-gray-700 hover:border-gray-200"
    };
  }
  return {
    dot: "bg-blue-500",
    text: "text-blue-700",
    pillActive: "bg-blue-50 text-blue-700 border-blue-200",
    pillHover: "hover:bg-blue-50 hover:text-blue-700 hover:border-blue-200"
  };
}

export { tsFormatter, timeFormatter, dateFormatter, logLevelPill, logLevelColors };
