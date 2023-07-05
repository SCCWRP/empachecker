const plotLoggerData = function (chartID, data, yAxisColumsToPlot) {

    var keys = yAxisColumsToPlot;

    var margin = { top: 20, right: 20, bottom: 110, left: 40 },
        margin2 = { top: 430, right: 20, bottom: 30, left: 40 },
        width = 960 - margin.left - margin.right,
        height = 500 - margin.top - margin.bottom,
        height2 = 500 - margin2.top - margin2.bottom;

    var x = d3.scaleTime().range([0, width]),
        x2 = d3.scaleTime().range([0, width]),
        y = d3.scaleLinear().range([height, 0]);

    var xAxis = d3.axisBottom(x),
        xAxis2 = d3.axisBottom(x2),
        yAxis = d3.axisLeft(y);

    var brush = d3.brushX()
        .extent([[0, 0], [width, height2]])
        .on("brush end", brushed);

    var line = d3.line()
        .x(function (d) { return x(d.samplecollectiontimestamp); })
        .y(function (d) { return y(d.value); });

    var svg = d3.select(`#${chartID}`).append("svg")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom);

    svg.append("defs").append("clipPath")
        .attr("id", "clip")
        .append("rect")
        .attr("width", width)
        .attr("height", height);

    const focus = svg.append("g")
        .attr("class", "focus")
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

    var context = svg.append("g")
        .attr("class", "context")
        .attr("transform", "translate(" + margin2.left + "," + margin2.top + ")");

    var color = d3.scaleOrdinal(d3.schemeCategory10);

    var parseTime = d3.timeParse("%Y-%m-%d %H:%M:%S");
    

    data.forEach(function (d) {
        d.samplecollectiontimestamp = parseTime(d.samplecollectiontimestamp);
    });

    var lines = keys.map(function (id) {
        if (!id) {
            console.error("id is not defined");
        }
    
        var values = data.map(function (d) {
            if (!d || d[id] === undefined || d[id] === null || isNaN(d[id])) {
                console.error("Invalid data for id: " + id, d);
                return null;
            }
            return {
                samplecollectiontimestamp: d.samplecollectiontimestamp,
                value: +d[id]
            };
        }).filter(v => v !== null);
    
        if (values.length === 0) {
            console.error("No valid values for id: " + id);
        }
    
        return {
            id: id,
            values: values
        };
    });
    
    console.log("lines")
    console.log(lines)
    

    x.domain(d3.extent(data, function (d) { return d.samplecollectiontimestamp; }));
    y.domain([
        d3.min(lines, function (c) { return d3.min(c.values, function (d) { return d.value; }); }),
        d3.max(lines, function (c) { return d3.max(c.values, function (d) { return d.value; }); })
    ]);
    x2.domain(x.domain());

    focus.selectAll('path')
        .data(lines)
        .enter().append('path')
        .attr('d', function (d) { return line(d.values); })
        .style('stroke', function (d) { return color(d.id); })
        .attr('clip-path', 'url(#clip)');

    focus.append("g")
        .attr("class", "axis axis--x")
        .attr("transform", "translate(0," + height + ")")
        .call(xAxis);

    focus.append("g")
        .attr("class", "axis axis--y")
        .call(yAxis);

    context.append("g")
        .attr("class", "axis axis--x")
        .attr("transform", "translate(0," + height2 + ")")
        .call(xAxis2);

    context.append("g")
        .attr("class", "brush")
        .call(brush)
        .call(brush.move, x.range());

    var legend = focus.selectAll('.legend')
        .data(color.domain())
        .enter().append('g')
        .attr('class', 'legend')
        .attr('transform', function (d, i) { return 'translate(0,' + i * 20 + ')'; });

    legend.append('rect')
        .attr('x', width - 18)
        .attr('width', 18)
        .attr('height', 18)
        .style('fill', color);

    legend.append('text')
        .attr('x', width - 24)
        .attr('y', 9)
        .attr('dy', '.35em')
        .style('text-anchor', 'end')
        .text(function (d) { return d; });

    function brushed(event) {
        // if (event.sourceEvent && event.sourceEvent.type === "zoom") return; // ignore brush-by-zoom
        // var s = event.selection || x2.range();
        // x.domain(s.map(x2.invert, x2));
        // focus.selectAll("path").attr("d", function (d) { return line(d.values); });
        // focus.select(".axis--x").call(xAxis);
        console.log('focus')
        console.log(focus)
    }
}