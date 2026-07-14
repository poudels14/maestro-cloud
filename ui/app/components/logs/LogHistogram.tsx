import { createEffect, onCleanup, onMount } from "solid-js";
import * as d3 from "d3";
import type { LogHistogramBucket } from "../../lib/api";

function LogHistogramChart(props: {
  data: LogHistogramBucket[];
  from: number;
  to: number;
  bucketMs: number;
  selectedTs?: number;
  onSelect: (bucket: LogHistogramBucket) => void;
}) {
  let containerRef: HTMLDivElement | undefined;
  let svgRef: SVGSVGElement | undefined;
  const height = 132;
  const margin = { top: 12, right: 12, bottom: 26, left: 42 };

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
      .call((axis) => axis.select(".domain").attr("stroke", "#e5e7eb"))
      .call((axis) => axis.selectAll(".tick line").attr("stroke", "#e5e7eb"))
      .call((axis) =>
        axis.selectAll(".tick text").attr("fill", "#9ca3af").attr("font-size", "10px")
      );
    graph
      .append("g")
      .call(
        d3
          .axisLeft(yScale)
          .ticks(3)
          .tickFormat((value) => d3.format("~s")(value.valueOf()))
      )
      .call((axis) => axis.select(".domain").remove())
      .call((axis) => axis.selectAll(".tick line").attr("x2", innerWidth).attr("stroke", "#f3f4f6"))
      .call((axis) =>
        axis.selectAll(".tick text").attr("fill", "#9ca3af").attr("font-size", "10px")
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
        .attr("fill", "#eef2ff");
    }

    const tooltip = graph.append("g").style("display", "none").attr("pointer-events", "none");
    tooltip
      .append("line")
      .attr("y1", 0)
      .attr("y2", innerHeight)
      .attr("stroke", "#9ca3af")
      .attr("stroke-dasharray", "3,3");
    const tooltipText = tooltip
      .append("text")
      .attr("y", -3)
      .attr("fill", "#374151")
      .attr("font-size", "11px");

    graph
      .selectAll(".log-bucket")
      .data(props.data)
      .join("rect")
      .attr("class", "log-bucket")
      .attr("x", (bucket) => bucketBounds(bucket).x)
      .attr("y", (bucket) => yScale(bucket.count))
      .attr("width", (bucket) => bucketBounds(bucket).width)
      .attr("height", (bucket) => Math.max(0, innerHeight - yScale(bucket.count)))
      .attr("rx", 1)
      .attr("fill", (bucket) => (bucket.ts === props.selectedTs ? "#4f46e5" : "#818cf8"))
      .attr("opacity", (bucket) => (bucket.count === 0 ? 0 : 0.9))
      .attr("role", (bucket) => (bucket.count > 0 ? "button" : null))
      .attr("tabindex", (bucket) => (bucket.count > 0 ? 0 : null))
      .attr("aria-label", (bucket) => {
        if (bucket.count === 0) return null;
        const bounds = bucketBounds(bucket);
        return `${bucket.count.toLocaleString()} logs from ${new Date(bounds.start).toLocaleString()} to ${new Date(bounds.end).toLocaleString()}`;
      })
      .style("cursor", (bucket) => (bucket.count > 0 ? "pointer" : "default"))
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
      .on("click", (_event, bucket) => {
        if (bucket.count > 0) props.onSelect(bucket);
      })
      .on("keydown", (event: KeyboardEvent, bucket) => {
        if (bucket.count > 0 && (event.key === "Enter" || event.key === " ")) {
          event.preventDefault();
          props.onSelect(bucket);
        }
      });

    if (maxCount === 0) {
      graph
        .append("text")
        .attr("x", innerWidth / 2)
        .attr("y", innerHeight / 2)
        .attr("text-anchor", "middle")
        .attr("fill", "#9ca3af")
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
