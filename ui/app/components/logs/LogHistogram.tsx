import { createEffect, onCleanup, onMount } from "solid-js";
import * as d3 from "d3";
import type { LogHistogramBucket } from "../../lib/api";

const LEVEL_PRIORITY = ["trace", "debug", "info", "warn", "error", "2xx", "3xx", "4xx", "5xx"];

function levelColor(level: string) {
  switch (level.toLowerCase()) {
    case "2xx":
      return "#52a47c";
    case "3xx":
      return "#8aa6c1";
    case "4xx":
      return "#e6b960";
    case "5xx":
      return "#cd656a";
    case "error":
    case "err":
    case "fatal":
    case "panic":
      return "#b23a3f";
    case "warn":
    case "warning":
      return "#c5861a";
    case "info":
      return "#93681c";
    case "debug":
    case "dbg":
      return "#4f6d8f";
    case "trace":
      return "#697386";
    default:
      return "#98a2b3";
  }
}

function levelRank(level: string) {
  const rank = LEVEL_PRIORITY.indexOf(level.toLowerCase());
  return rank < 0 ? -1 : rank;
}

function bucketLevels(bucket: LogHistogramBucket) {
  const entries = Object.entries(bucket.levels ?? {}).filter(([, count]) => count > 0);
  const categorized = entries.reduce((total, [, count]) => total + count, 0);
  if (categorized < bucket.count) entries.push(["other", bucket.count - categorized]);
  return entries.sort(([left], [right]) => {
    const rank = levelRank(left) - levelRank(right);
    return rank || left.localeCompare(right);
  });
}

function LogHistogramChart(props: {
  data: LogHistogramBucket[];
  from: number;
  to: number;
  bucketMs: number;
  selectedTs?: number;
  onSelectInterval?: (bucket: LogHistogramBucket) => void;
  onSelect?: (bucket: LogHistogramBucket, level: string) => void;
}) {
  let containerRef: HTMLDivElement | undefined;
  let svgRef: SVGSVGElement | undefined;
  const height = 80;
  const margin = { top: 6, right: 12, bottom: 20, left: 42 };

  const render = () => {
    if (!containerRef || !svgRef) return;
    const width = containerRef.clientWidth;
    if (width <= 0 || props.to <= props.from || props.bucketMs <= 0) return;

    const innerWidth = Math.max(1, width - margin.left - margin.right);
    const innerHeight = height - margin.top - margin.bottom;
    const svg = d3.select(svgRef);
    svg.selectAll("*").remove();
    svg.attr("width", width).attr("height", height);

    const xScale = d3.scaleTime().domain([props.from, props.to]).range([0, innerWidth]);
    const maxCount = d3.max(props.data, (bucket) => bucket.count) ?? 0;
    const yScale = d3
      .scaleLinear()
      .domain([0, maxCount || 1])
      .nice()
      .range([innerHeight, 0]);
    const graph = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);

    const rangeMs = props.to - props.from;
    const timeFormat =
      rangeMs > 2 * 24 * 60 * 60 * 1000 ? d3.timeFormat("%b %d") : d3.timeFormat("%H:%M");
    graph
      .append("g")
      .attr("transform", `translate(0,${innerHeight})`)
      .call(
        d3
          .axisBottom(xScale)
          .ticks(6)
          .tickFormat((value) => timeFormat(value as Date))
      )
      .call((axis) => axis.select(".domain").attr("stroke", "#dce1e8"))
      .call((axis) => axis.selectAll(".tick line").attr("stroke", "#dce1e8"))
      .call((axis) =>
        axis.selectAll(".tick text").attr("fill", "#98a2b3").attr("font-size", "10px")
      );
    graph
      .append("g")
      .call(
        d3
          .axisLeft(yScale)
          .ticks(2)
          .tickFormat((value) => d3.format("~s")(value.valueOf()))
      )
      .call((axis) => axis.select(".domain").remove())
      .call((axis) => axis.selectAll(".tick line").attr("x2", innerWidth).attr("stroke", "#eaedf2"))
      .call((axis) =>
        axis.selectAll(".tick text").attr("fill", "#98a2b3").attr("font-size", "10px")
      );

    const bucketBounds = (bucket: LogHistogramBucket) => {
      const start = Math.max(props.from, bucket.ts);
      const end = Math.min(props.to, bucket.ts + props.bucketMs);
      const x = Math.max(0, xScale(start));
      const endX = Math.min(innerWidth, xScale(end));
      return { start, end, x, width: Math.max(1, endX - x - 1) };
    };

    const selected = props.data.find((bucket) => bucket.ts === props.selectedTs);
    if (selected) {
      const bounds = bucketBounds(selected);
      graph
        .append("rect")
        .attr("x", bounds.x)
        .attr("y", 0)
        .attr("width", bounds.width + 1)
        .attr("height", innerHeight)
        .attr("fill", "#f5ebd3");
    }

    const tooltip = graph.append("g").style("display", "none").attr("pointer-events", "none");
    tooltip
      .append("line")
      .attr("y1", 0)
      .attr("y2", innerHeight)
      .attr("stroke", "#98a2b3")
      .attr("stroke-dasharray", "3,3");
    const tooltipText = tooltip
      .append("text")
      .attr("y", -3)
      .attr("fill", "#3a4352")
      .attr("font-size", "11px");

    graph
      .selectAll(".log-bucket-hit-area")
      .data(props.data)
      .join("rect")
      .attr("class", "log-bucket-hit-area")
      .attr("x", (bucket) => bucketBounds(bucket).x)
      .attr("y", 0)
      .attr("width", (bucket) => bucketBounds(bucket).width)
      .attr("height", innerHeight)
      .attr("fill", "transparent")
      .attr("role", "button")
      .attr("tabindex", 0)
      .attr("aria-label", (bucket) => {
        const bounds = bucketBounds(bucket);
        return `Select ${bucket.count.toLocaleString()} logs from ${new Date(bounds.start).toLocaleString()} to ${new Date(bounds.end).toLocaleString()}`;
      })
      .style("cursor", props.onSelectInterval ? "pointer" : "default")
      .on("mouseenter", (_event, bucket) => {
        const bounds = bucketBounds(bucket);
        const x = bounds.x + bounds.width / 2;
        const start = new Date(bounds.start);
        const end = new Date(bounds.end);
        const label = `${bucket.count.toLocaleString()} logs · ${d3.timeFormat("%b %d %H:%M")(start)}–${d3.timeFormat("%H:%M")(end)}`;
        tooltip.style("display", null);
        tooltip.select("line").attr("x1", x).attr("x2", x);
        tooltipText
          .attr("x", x < innerWidth / 2 ? x + 5 : x - 5)
          .attr("text-anchor", x < innerWidth / 2 ? "start" : "end")
          .text(label);
      })
      .on("mouseleave", () => tooltip.style("display", "none"))
      .on("click", (_event, bucket) => props.onSelectInterval?.(bucket))
      .on("keydown", (event: KeyboardEvent, bucket) => {
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          props.onSelectInterval?.(bucket);
        }
      });

    const segments = props.data.flatMap((bucket) => {
      let offset = 0;
      return bucketLevels(bucket).map(([level, count]) => {
        const segment = { bucket, level, count, start: offset, end: offset + count };
        offset += count;
        return segment;
      });
    });

    graph
      .selectAll(".log-bucket-segment")
      .data(segments)
      .join("rect")
      .attr("class", "log-bucket-segment")
      .attr("x", (segment) => bucketBounds(segment.bucket).x)
      .attr("y", (segment) => yScale(segment.end))
      .attr("width", (segment) => bucketBounds(segment.bucket).width)
      .attr("height", (segment) => Math.max(0, yScale(segment.start) - yScale(segment.end)))
      .attr("rx", 1)
      .attr("fill", (segment) => levelColor(segment.level))
      .attr("opacity", 0.9)
      .attr("role", "button")
      .attr("tabindex", 0)
      .attr("aria-label", (segment) => {
        const bounds = bucketBounds(segment.bucket);
        return `${segment.count.toLocaleString()} ${segment.level} logs from ${new Date(bounds.start).toLocaleString()} to ${new Date(bounds.end).toLocaleString()}`;
      })
      .style("cursor", props.onSelect ? "pointer" : "default")
      .on("mouseenter", (_event, segment) => {
        const bounds = bucketBounds(segment.bucket);
        const x = bounds.x + bounds.width / 2;
        const start = new Date(bounds.start);
        const end = new Date(bounds.end);
        const label = `${segment.count.toLocaleString()} ${segment.level} logs · ${segment.bucket.count.toLocaleString()} total · ${d3.timeFormat("%b %d %H:%M")(start)}–${d3.timeFormat("%H:%M")(end)}`;
        tooltip.style("display", null);
        tooltip.select("line").attr("x1", x).attr("x2", x);
        tooltipText
          .attr("x", x < innerWidth / 2 ? x + 5 : x - 5)
          .attr("text-anchor", x < innerWidth / 2 ? "start" : "end")
          .text(label);
      })
      .on("mouseleave", () => tooltip.style("display", "none"))
      .on("click", (_event, segment) => {
        props.onSelect?.(segment.bucket, segment.level);
      })
      .on("keydown", (event: KeyboardEvent, segment) => {
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          props.onSelect?.(segment.bucket, segment.level);
        }
      });

    if (maxCount === 0) {
      graph
        .append("text")
        .attr("x", innerWidth / 2)
        .attr("y", innerHeight / 2)
        .attr("text-anchor", "middle")
        .attr("fill", "#98a2b3")
        .attr("font-size", "11px")
        .text("No matching logs in this range");
    }
  };

  onMount(() => {
    render();
    const observer = new ResizeObserver(render);
    if (containerRef) observer.observe(containerRef);
    onCleanup(() => observer.disconnect());
  });

  createEffect(() => {
    props.data;
    props.from;
    props.to;
    props.bucketMs;
    props.selectedTs;
    render();
  });

  return (
    <div ref={containerRef} class="w-full">
      <svg ref={svgRef} class="block w-full" />
    </div>
  );
}

export { LogHistogramChart };
