import { createEffect, onCleanup, onMount } from "solid-js";
import * as d3 from "d3";

type DataPoint = { ts: number; value: number };

export function TimelineChart(props: {
  data: DataPoint[];
  label: string;
  color?: string;
  yFormat?: (v: number) => string;
  height?: number;
  xMin?: number;
  xMax?: number;
  secondarySeries?: { data: DataPoint[]; color: string; label: string };
}) {
  let containerRef: HTMLDivElement | undefined;
  let svgRef: SVGSVGElement | undefined;

  const color = () => props.color ?? "#6366f1";
  const height = () => props.height ?? 180;

  const margin = { top: 12, right: 16, bottom: 28, left: 52 };

  const render = () => {
    if (!svgRef || !containerRef) return;

    const width = containerRef.clientWidth;
    if (width <= 0) return;

    const innerWidth = width - margin.left - margin.right;
    const innerHeight = height() - margin.top - margin.bottom;

    const svg = d3.select(svgRef);
    svg.selectAll("*").remove();
    svg.attr("width", width).attr("height", height());

    const allData = props.secondarySeries
      ? [...props.data, ...props.secondarySeries.data]
      : props.data;

    if (allData.length === 0) {
      svg
        .append("text")
        .attr("x", width / 2)
        .attr("y", height() / 2)
        .attr("text-anchor", "middle")
        .attr("fill", "#9ca3af")
        .attr("font-size", "12px")
        .text("No data");
      return;
    }

    const dataExtent = d3.extent(allData, (d) => d.ts) as [number, number];
    const xMin = props.xMin ?? dataExtent[0];
    const xMax = props.xMax ?? dataExtent[1];
    const yMax = d3.max(allData, (d) => d.value) ?? 0;

    const xScale = d3.scaleTime().domain([xMin, xMax]).range([0, innerWidth]);

    const yScale = d3
      .scaleLinear()
      .domain([0, yMax * 1.1 || 1])
      .range([innerHeight, 0])
      .nice();

    const g = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);

    g.append("g")
      .attr("transform", `translate(0,${innerHeight})`)
      .call(
        d3
          .axisBottom(xScale)
          .ticks(6)
          .tickFormat((d) => d3.timeFormat("%H:%M")(d as Date))
      )
      .call((g) => g.select(".domain").attr("stroke", "#e5e7eb"))
      .call((g) => g.selectAll(".tick line").attr("stroke", "#e5e7eb"))
      .call((g) => g.selectAll(".tick text").attr("fill", "#9ca3af").attr("font-size", "10px"));

    const yFormat = props.yFormat ?? ((v: number) => String(Math.round(v)));
    g.append("g")
      .call(
        d3
          .axisLeft(yScale)
          .ticks(4)
          .tickFormat(((d: d3.NumberValue) => yFormat(d.valueOf())) as (
            d: d3.NumberValue,
            i: number
          ) => string)
      )
      .call((g) => g.select(".domain").remove())
      .call((g) => g.selectAll(".tick line").attr("x2", innerWidth).attr("stroke", "#f3f4f6"))
      .call((g) => g.selectAll(".tick text").attr("fill", "#9ca3af").attr("font-size", "10px"));

    const line = d3
      .line<DataPoint>()
      .x((d) => xScale(d.ts))
      .y((d) => yScale(d.value))
      .curve(d3.curveMonotoneX);

    const area = d3
      .area<DataPoint>()
      .x((d) => xScale(d.ts))
      .y0(innerHeight)
      .y1((d) => yScale(d.value))
      .curve(d3.curveMonotoneX);

    g.append("path")
      .datum(props.data)
      .attr("fill", color())
      .attr("fill-opacity", 0.08)
      .attr("d", area);

    g.append("path")
      .datum(props.data)
      .attr("fill", "none")
      .attr("stroke", color())
      .attr("stroke-width", 1.5)
      .attr("d", line);

    if (props.secondarySeries) {
      const secColor = props.secondarySeries.color;
      g.append("path")
        .datum(props.secondarySeries.data)
        .attr("fill", secColor)
        .attr("fill-opacity", 0.08)
        .attr("d", area);

      g.append("path")
        .datum(props.secondarySeries.data)
        .attr("fill", "none")
        .attr("stroke", secColor)
        .attr("stroke-width", 1.5)
        .attr("d", line);
    }

    const tooltip = g.append("g").style("display", "none");
    tooltip
      .append("line")
      .attr("y1", 0)
      .attr("y2", innerHeight)
      .attr("stroke", "#d1d5db")
      .attr("stroke-dasharray", "3,3");
    tooltip
      .append("circle")
      .attr("r", 3)
      .attr("fill", color())
      .attr("stroke", "white")
      .attr("stroke-width", 1.5);
    const tooltipText = tooltip
      .append("text")
      .attr("fill", "#374151")
      .attr("font-size", "11px")
      .attr("text-anchor", "start")
      .attr("dy", -8);

    const overlay = g
      .append("rect")
      .attr("width", innerWidth)
      .attr("height", innerHeight)
      .attr("fill", "none")
      .attr("pointer-events", "all");

    const bisect = d3.bisector<DataPoint, number>((d) => d.ts).left;

    overlay
      .on("mousemove", (event: MouseEvent) => {
        const [mx] = d3.pointer(event);
        const ts = xScale.invert(mx).getTime();
        const idx = bisect(props.data, ts, 1);
        const d0 = props.data[idx - 1];
        const d1 = props.data[idx];
        if (!d0) return;
        const d = d1 && ts - d0.ts > d1.ts - ts ? d1 : d0;
        const x = xScale(d.ts);
        const y = yScale(d.value);

        tooltip.style("display", null);
        tooltip.select("line").attr("x1", x).attr("x2", x);
        tooltip.select("circle").attr("cx", x).attr("cy", y);
        tooltipText
          .attr("x", x + 6)
          .attr("y", y)
          .text(`${yFormat(d.value)} · ${d3.timeFormat("%H:%M:%S")(new Date(d.ts))}`);
      })
      .on("mouseleave", () => {
        tooltip.style("display", "none");
      });

    if (props.secondarySeries) {
      const legendY = -2;
      const legendG = g.append("g").attr("transform", `translate(${innerWidth - 120},${legendY})`);
      legendG
        .append("line")
        .attr("x1", 0)
        .attr("x2", 12)
        .attr("y1", 0)
        .attr("y2", 0)
        .attr("stroke", color())
        .attr("stroke-width", 1.5);
      legendG
        .append("text")
        .attr("x", 16)
        .attr("y", 4)
        .attr("fill", "#6b7280")
        .attr("font-size", "10px")
        .text(props.label);
      legendG
        .append("line")
        .attr("x1", 60)
        .attr("x2", 72)
        .attr("y1", 0)
        .attr("y2", 0)
        .attr("stroke", props.secondarySeries.color)
        .attr("stroke-width", 1.5);
      legendG
        .append("text")
        .attr("x", 76)
        .attr("y", 4)
        .attr("fill", "#6b7280")
        .attr("font-size", "10px")
        .text(props.secondarySeries.label);
    }
  };

  onMount(() => {
    render();
    const resizeObserver = new ResizeObserver(() => render());
    if (containerRef) resizeObserver.observe(containerRef);
    onCleanup(() => resizeObserver.disconnect());
  });

  createEffect(() => {
    props.data;
    props.xMin;
    props.xMax;
    props.secondarySeries?.data;
    render();
  });

  return (
    <div ref={containerRef} class="w-full">
      <svg ref={svgRef} class="w-full" />
    </div>
  );
}
