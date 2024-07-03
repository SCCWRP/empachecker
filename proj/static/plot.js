class Plot {
    constructor(data, chartID) {
        this.data = data;
        this.chartID = chartID;
        this.filteredData = data;
    }

    set x(value) {
        this._x = value;
    }
    get x() {
        return this._x;
    }

    set y(value) {
        this._y = value;
    }
    get y() {
        return this._y;
    }


    set xDataType(dtype) {
        const acceptableDataTypes = ['Numeric', 'Time', 'Categorical'];
        console.assert(acceptableDataTypes.includes(dtype), `Error in setting datatype in xDataType - must be ${acceptableDataTypes.join(', ')}`)
        this._xDataType = dtype;
    }

    get xDataType() {
        return this._xDataType;
    }

    set yDataType(dtype) {
        const acceptableDataTypes = ['Numeric', 'Time', 'Categorical'];
        console.assert(acceptableDataTypes.includes(dtype), `Error in setting datatype in yDataType - must be ${acceptableDataTypes.join(', ')}`)
        this._yDataType = dtype;
    }

    get yDataType() {
        return this._yDataType;
    }



    set margins(margin) {
        this._margins = margin;
    }

    get margins() {
        return this._margins;
    }

    set visHeight(height) {
        this._visHeight = height;
    }
    set visWidth(width) {
        this._visWidth = width;
    }
    get visHeight() {
        return this._visHeight;
    }
    get visWidth() {
        return this._visWidth;
    }

    set canvasHeight(height) {
        this._canvasHeight = height;
        this.visHeight = this._canvasHeight - this._margins.bottom - this._margins.top;
        if (this.svg) {
            this.svg.attr('height', this._canvasHeight)
        }
        if (this.chartArea) {
            this.chartArea.attr('height', this._visHeight)
        }
    }

    set canvasWidth(width) {
        this._canvasWidth = width;
        this.visWidth = this._canvasWidth - this._margins.left - this._margins.right;
        if (this.svg) {
            this.svg.attr('width', this._canvasWidth)
        }
        if (this.chartArea) {
            this.chartArea.attr('width', this._visWidth)
        }
    }

    get canvasHeight() {
        return this._canvasHeight
    }

    get canvasWidth() {
        return this._canvasWidth;
    }
    set xAxisTextRotation(n) {
        this._xAxisTextRotation = n;
    }

    set colorPallete(colors) {
        this._colorPallete = colors;
    }
    get colorPallete() {
        return this._colorPallete;
    }

    get svgID() {
        return `${this.chartID}-svg`
    }

    addTitle(title, fontSize = "1.5rem") {
        const vis = this;
        vis.svg.append('g')
            .attr('id', 'plot-main-title')
            .append('text')
            .attr('transform', `translate(${this.visWidth / 2}, ${this.margins.top / 2})`)
            .style('font-size', fontSize)
            .text(title)
    }

    drawAxes(
        {
            xAxisFontSize = 14,
            xAxisRotation = -65,
            yAxisFontSize = 14,
            yAxisRotation = 0,
            xAxisLabel = '',
            yAxisLabel = ''
        } = {}
    ) {
        const vis = this;
        const xAxisGroup = this.chartArea.append("g")
            .attr("transform", `translate(0, ${this._visHeight})`)

        const xTickFormat = d3.timeFormat("%m/%d %H:%M");

        //Should probably add args for the tick formatting to give that option
        const xAxis = d3.axisBottom(this.xScale)
        if (xTickFormat) {
            xAxis.tickFormat(xTickFormat)
        }

        xAxisGroup.call(xAxis)
            .selectAll("text")
            .style("text-anchor", "end")
            .style("font-size", `${xAxisFontSize}px`)
            .attr("dx", "-1em")
            .attr("dy", ".2em")
            .attr("transform", `rotate(${xAxisRotation})`);

        const yAxisGroup = this.chartArea.append("g")
        yAxisGroup.call(d3.axisLeft(this.yScale))
            .selectAll("text")
            .style("font-size", `${yAxisFontSize}px`)
            .attr("transform", `rotate(${yAxisRotation})`);

        this.svg.append('g')
            .attr('class', 'x-axis-label')
            .append('text')
            .attr('transform', `translate(${(vis._visWidth / 2) + vis.margins.left},${vis._canvasHeight - vis.margins.bottom / 5})`)
            .text(xAxisLabel)

        this.svg.append('g')
            .attr('class', 'y-axis-label')
            .append('text')
            .attr('text-anchor', 'middle')
            .attr('transform', `rotate(-90) translate(${-(vis.margins.top + (vis._visHeight / 2))}, ${(vis.margins.left * 0.3)})`)
            .text(yAxisLabel)
    }

    setScale(x, y, xDataType, yDataType, xScaleType = 'Linear', yScaleType = 'Linear', padding = 0.2) {
        const acceptableDataTypes = ['Numeric', 'Time', 'Categorical'];
        console.assert(acceptableDataTypes.includes(xDataType), `Error in use of PathPlot class draw method - xDataType must be ${acceptableDataTypes.join(', ')}`)
        console.assert(acceptableDataTypes.includes(yDataType), `Error in use of PathPlot class draw method - yDataType must be ${acceptableDataTypes.join(', ')}`)

        if (xDataType == 'Time') {
            this.xScale = d3.scaleTime()
                .domain([d3.min(this.filteredData, d => d[x]), d3.max(this.filteredData, d => d[x])])
                .range([0, this._visWidth])
        }
        if (yDataType == 'Time') {
            this.yScale = d3.scaleTime()
                .domain([d3.min(this.filteredData, d => d[y]), d3.max(this.filteredData, d => d[y])])
                .range([0, this._visHeight])
        }

        if (xDataType == 'Categorical') {
            this.xScale = d3.scaleBand()
                .domain(this.filteredData.map(d => d[x]))
                .range([0, this._visWidth])
                .paddingInner(padding)
                .paddingOuter(padding);

        }
        if (yDataType == 'Categorical') {
            this.yScale = d3.scaleBand()
                .domain(this.filteredData.map(d => d[y]))
                .range([this._visHeight, 0])
                .paddingInner(padding)
                .paddingOuter(padding);
        }

        if (xDataType == 'Numeric') {
            this.xScale = d3.scaleLinear()
                .domain([d3.min(this.filteredData, d => d[x]), d3.max(this.filteredData, d => d[x])])
                .range([0, this._visWidth])
        }

        if (yDataType == 'Numeric') {
            this.yScale = d3.scaleLinear()
                .domain([d3.min(this.filteredData, d => d[y]), d3.max(this.filteredData, d => d[y])])
                .range([this._visHeight, 0])
        }

    }

    init() {
        console.log("init")
        if (!this._canvasHeight) {
            console.warn("Warning in use of Plot class - canvas height not defined")
            this._canvasHeight = document.getElementById(this.chartID).getBoundingClientRect().height;
        }
        if (!this._canvasWidth) {
            console.warn("Warning in use of Plot class - canvas width not defined")
            this._canvasWidth = document.getElementById(this.chartID).getBoundingClientRect().width;
        }
        if (!this._margins) {
            console.warn("Warning in use of Plot class - margins not defined")
            this._margins = {
                top: Math.round(this._canvasHeight * 0.05),
                bottom: Math.round(this._canvasHeight * 0.05),
                left: Math.round(this._canvasWidth * 0.05),
                right: Math.round(this._canvasWidth * 0.05)
            }
        }

        this.visWidth = this._canvasWidth - this._margins.left - this._margins.right;
        this.visHeight = this._canvasHeight - this._margins.bottom - this._margins.top;

        this.svg = d3.select(`#${this.chartID}`)
            .append('svg')
            .attr('width', this._canvasWidth)
            .attr('height', this._canvasHeight)
            .attr('id', `${this.chartID}-svg`)
            .style('background-color', 'white')

        this.chartArea = this.svg.append('g')
            .attr('width', this._visWidth)
            .attr('height', this._visHeight)
            .attr('transform', `translate(${this._margins.left},${this._margins.top})`)
    }

    download(filename) {
        const vis = this;
        const svgID = vis.svgID;

        const svgElem = document.getElementById(svgID);
        const serializer = new XMLSerializer();

        let svgData = serializer.serializeToString(svgElem);
        svgData = '<?xml version="1.0" standalone="no"?>\r\n' + svgData;
        const svgBlob = new Blob([svgData], {
            type: 'image/svg+xml;charset=utf-8',
        });
        let DOMURL = window.URL || window.webkitURL || window;
        const url = DOMURL.createObjectURL(svgBlob);

        const img = new Image();
        img.onload = () => {
            const canvas = document.createElement('canvas');
            const ctx = canvas.getContext('2d');
            canvas.width = vis._canvasWidth;
            canvas.height = vis._canvasHeight;
            ctx.drawImage(img, 0, 0, canvas.width, canvas.height);
            DOMURL.revokeObjectURL(url);

            const imgURI = canvas
                .toDataURL('image/png')
                .replace('image/png', 'image/octet-stream');

            //download(imgURI);
            let download = document.createElement('a');
            download.href = imgURI;
            download.download = filename;
            download.click();
            download.remove();
        };
        img.onerror = (e) => {
            console.error('Image not loaded', e);
        };

        img.src = url;
    }
}


class PathPlot extends Plot {
    constructor(data, chartID, x, y) {
        super(data, chartID);
        this.filteredData = data;
        this.x = x;
        this.y = y;
    }

    draw(
        {
            x = this.x,
            y = this.y,
            color = 'black',
            strokeWidth = 1.5,
            xDataType = this.xDataType,
            yDataType = this.yDataType,
            xScaleType = 'Linear',
            yScaleType = 'Linear',
            xAxisLabel = '',
            yAxisLabel = ''
        } = {}
    ) {

        const vis = this;

        console.assert(this._margins, "Error in use of PathPlot - Margins are not defined but an attempt to draw was made. Did you call the init() method?")
        console.assert(this.xDataType, "Error in use of PathPlot - xDataType is not defined, but an attempt to draw was made.")
        console.assert(this.yDataType, "Error in use of PathPlot - yDataType is not defined, but an attempt to draw was made.")

        const margin = this._margins;
        const visWidth = this._visWidth;
        const visHeight = this._visHeight;
        const canvasWidth = this._canvasWidth;
        const canvasHeight = this._canvasHeight;
        // const colorPallete = this.colorPallete;

        this.setScale(x, y, xDataType, yDataType);

        const xScale = this.xScale;
        const yScale = this.yScale;

        const line = d3.line()
            .x(d => {
                return this.xScale(d[this.x]);
            })
            .y(d => {
                return this.yScale(d[this.y]);
            });

        const pathData = line(vis.data);

        this.chartArea.selectAll('path')
            .data(vis.data)
            .join(
                (enter) => enter.append('path')
                    .attr('opacity', 0)
                    .attr('stroke', 'black'),
                (update) => update,
                (exit) => exit.remove()
            )
            .transition()
            .duration(700)
            .attr('fill', 'none')
            .attr('opacity', 1)
            .attr('stroke', d => {
                // return colorMap.get(d[0]);
                return 'black'
            })
            .attr('stroke-width', strokeWidth)
            .attr('d', pathData)
            ;


        this.drawAxes(
            {
                xAxisLabel: xAxisLabel,
                yAxisLabel: yAxisLabel
            }
        );



    }

    update() {
        // this.chartArea.selectAll('text').transition().duration(500).remove()
        // this.chartArea.selectAll('.tick').transition().duration(500).remove()
        //this.chartArea.selectAll('text').transition().duration(1000).style('opacity', 0).remove()
        this.chartArea.selectAll('text').remove()
        this.chartArea.selectAll('.tick').remove()
        this.draw()
    }
}



/* ------------------------------------------------------------------------------------------------------------------------- */


class CanvasPathPlot extends Plot {
    constructor(data, canvasId) {
        this.canvas = canvasId
    }

    init() {
        // super.init();
        console.log("init")
        if (!this._canvasHeight) {
            console.warn("Warning in use of Plot class - canvas height not defined")
            this._canvasHeight = document.getElementById(this.chartID).getBoundingClientRect().height;
        }
        if (!this._canvasWidth) {
            console.warn("Warning in use of Plot class - canvas width not defined")
            this._canvasWidth = document.getElementById(this.chartID).getBoundingClientRect().width;
        }
        if (!this._margins) {
            console.warn("Warning in use of Plot class - margins not defined")
            this._margins = {
                top: Math.round(this._canvasHeight * 0.05),
                bottom: Math.round(this._canvasHeight * 0.05),
                left: Math.round(this._canvasWidth * 0.05),
                right: Math.round(this._canvasWidth * 0.05)
            }
        }

        this.visWidth = this._canvasWidth - this._margins.left - this._margins.right;
        this.visHeight = this._canvasHeight - this._margins.bottom - this._margins.top;


        // Overlay an SVG for axes drawing on the canvas
        this.svg = d3.select(`#${this.chartID}`)
            .append('svg')
            .style('background-color', `rgba(255,255,255,0.2)`)
            .attr('width', this._canvasWidth)
            .attr('height', this._canvasHeight)

        this.canvas = d3.select(`#${this.chartID}`)
            .append('canvas')
            .style('position', 'absolute')
            .style('top', 0)
            .style('left', 0)
            .attr('width', this.visWidth)
            .attr('height', this.visHeight)
            .style('background-color', `rgba(0,0,255,0.2)`)
            .attr('transform', `translate(${this._margins.left},${this._margins.top})`)
            .node();

        this.context = this.canvas.getContext('2d');
    }

    draw(
        {
            x = this.x,
            y = this.y,
            color = 'black',
            strokeWidth = 1.5,
            xDataType = this.xDataType,
            yDataType = this.yDataType,
            xScaleType = 'Linear',
            yScaleType = 'Linear',
            xAxisLabel = '',
            yAxisLabel = ''
        } = {}
    ) {
        if (!this.xDataType || !this.yDataType) {
            console.error('Datatypes have not been set!');
            return;
        }
        if (!this.x || !this.y) {
            console.error('x and y values have not been set!');
            return;
        }

        // Clear the old drawing
        this.context.clearRect(0, 0, this._canvasWidth, this._canvasHeight);


        x = this.x;
        y = this.y;
        xDataType = this.xDataType;
        yDataType = this.yDataType;

        this.setScale(x, y, xDataType, yDataType);

        // Start a new path
        this.context.beginPath();


        
        this.filteredData.forEach((d, i) => {
            // Move to the first point
            if (i === 0) this.context.moveTo(this.xScale(d[this.x]), this.yScale(d[this.y]));
            else this.context.lineTo(this.xScale(d[this.x]), this.yScale(d[this.y]));
        });

        this.context.strokeStyle = 'steelblue';
        this.context.stroke();

        this.drawAxes({
            xAxisLabel: xAxisLabel,
            yAxisLabel: yAxisLabel
        });

        // If you want to fill the area under the line
        // this.context.lineTo(this.xScale(this.filteredData[this.filteredData.length - 1][this.x]), this.canvasHeight);
        // this.context.lineTo(this.xScale(this.filteredData[0][this.x]), this.canvasHeight);
        // this.context.closePath();

        // this.context.fillStyle = 'rgba(70, 130, 180, 0.4)';  // steelblue with transparency
        // this.context.fill();
    }

    drawAxes({
        xAxisFontSize = 20,
        yAxisFontSize = 20,
        xAxisLabel = '',
        yAxisLabel = '',
        numXTicks = 10,
        numYTicks = 10
    } = {}) {
        const xAxis = d3.axisBottom(this.xScale)
            .ticks(numXTicks);

        const yAxis = d3.axisLeft(this.yScale)
            .ticks(numYTicks);

        // Append the x and y axes to the SVG
        this.svg.append('g')
            .attr('transform', `translate(0, ${this.visHeight})`)
            .call(xAxis)
            .append('text')
            .attr('y', -10)
            .attr('x', this.visWidth)
            .attr('text-anchor', 'end')
            .attr('fill', '#000')
            .text(xAxisLabel);

        this.svg.append('g')
            .call(yAxis)
            .append('text')
            .attr('y', 6)
            .attr('dy', '0.71em')
            .attr('text-anchor', 'end')
            .attr('fill', '#000')
            .text(yAxisLabel);

        // Update the CSS for the text elements for the axes
        this.svg.selectAll('text')
            .style('font-size', `${xAxisFontSize}px`);
    }


    // Other methods remain the same...
}



// The idea is for this function to craete a plot on an HTML5 canvas rather than SVG for performance reasons, and handling larger sets of data
function createPlot(
    data, xVal, yVal, canvasId = 'canvas', canvasWidth = 960, canvasHeight = 500, margins = { top: 20, right: 20, bottom: 30, left: 50 }, yAxisLabel = null, xAxisLabel = null
) {
    
    const width = canvasWidth - margins.left - margins.right,
        height = canvasHeight - margins.top - margins.bottom;

    const canvas = d3.select(`#${canvasId}`)
        .attr("width", canvasWidth)
        .attr("height", canvasHeight)
        .node();

    const context = canvas.getContext('2d');

    // Clear the canvas before replotting
    context.clearRect(0, 0, canvas.width, canvas.height);

    const x = d3.scaleTime().range([0, width]);
    const y = d3.scaleLinear().range([height, 0]);

    const line = d3.line()
        .x(d => x(new Date(d[xVal])))
        .y(d => y(d[yVal]))
        .context(context);

    x.domain(d3.extent(data, d => new Date(d[xVal])));
    y.domain([0, d3.max(data, d => d[yVal])]);

    context.translate(margins.left, margins.top);

    context.beginPath();
    line(data);
    context.lineWidth = 1.5;
    context.strokeStyle = 'steelblue';
    context.stroke();

    // Draw x-axis manually
    context.beginPath();
    context.moveTo(0, height);
    context.lineTo(width, height);
    context.strokeStyle = 'black';
    context.stroke();

    // Draw y-axis manually
    context.beginPath();
    context.moveTo(0, 0);
    context.lineTo(0, height);
    context.strokeStyle = 'black';
    context.stroke();


    // Draw x-axis ticks and labels
    //const xTicksCount = Math.round(width / 75);
    const xTicks = x.ticks();
    const xTickFormat = d3.timeFormat("%Y-%m-%d %H:%M:%S");
    xTicks.forEach(tick => {
        const xPos = x(tick);
        context.beginPath();
        context.moveTo(xPos, height);
        context.lineTo(xPos, height + 6); // 6 is the length of the tick
        context.stroke();

        // rotate context, draw label, then unrotate
        context.save();
        context.translate(xPos, height + 20);
        context.rotate(-45 * Math.PI / 180);
        context.textAlign = 'right';
        context.textBaseline = 'middle';
        context.fillText(xTickFormat(tick), 0, 0);
        context.restore();
    });


    // Draw y-axis ticks and labels
    const yTicks = y.ticks();
    const yTickFormat = y.tickFormat();
    yTicks.forEach(tick => {
        const yPos = y(tick);
        context.beginPath();
        context.moveTo(0, yPos);
        context.lineTo(-6, yPos); // 6 is the length of the tick
        context.stroke();
        // context.fillText(yTickFormat(tick), -10, yPos + 3); // 10 is the distance from the tick to the label, 3 is vertical adjustment
        context.fillText(yTickFormat(tick), - (margins.left / 5), yPos + 3); // 10 is the distance from the tick to the label, 3 is vertical adjustment
    });

    // text label for the x axis
    xAxisLabel = xAxisLabel ?? xVal;
    context.font = "30px Arial";
    context.textAlign = 'center';
    context.fillText(xAxisLabel, width / 2, height + (margins.bottom * 0.7) + 10); // increased distance to avoid overlap with x-axis labels

    // text label for the y axis
    yAxisLabel = yAxisLabel ?? yVal;
    context.save();
    context.rotate(-Math.PI / 2);
    context.textAlign = 'center';
    context.fillText(yAxisLabel, -height / 2, -margins.left / 2);
    context.restore();

    brushHandler({
        data: data,
        canvasID : canvasId,
        canvasWidth : canvasWidth,
        canvasHeight : canvasHeight
    }) 
}

// GPT provided code for a 1D X Axis brush
// This brush handler gets added every time the plot gets redrawn
// The reason why is so it can have access to the filtered dataset, otherwise the user's brushing action will filter the original dataset, no matter what the plot is actually displaying
// To be honest at this point, the code has gotten out of hand and needs refactoring
// I think it will make a lot more sense to make this canvas D3 plot class based rather than function based
// However, i have already spent a lot more time on this than i originally intended, and the product will work well enough for what jan and her partners are looking for
// So probably not worth the time and effort at this particular moment.
// However, it is something to keep in mind at the very least
const brushHandler = function (
    {

        data = [],
        canvasID = 'logger-canvas',
        canvasWidth = 500,
        canvasHeight = 500,
        marginsPct = {
            top: 0.05,
            right: 0.02,
            bottom: 0.25,
            left: 0.10
        },
        xVal = 'samplecollectiontimestamp'
    } = {}
) 
{

    const canvas = document.querySelector(`#${canvasID}`);
    const canvasd3 = d3.select(`#${canvasID}`).node();
    const context = canvasd3.getContext('2d');

    
    let margins = {
        top: canvasHeight * marginsPct.top,
        bottom: canvasHeight * marginsPct.bottom,
        left: canvasWidth * marginsPct.left,
        right: canvasWidth * marginsPct.right
    }

    let width = canvasWidth - margins.left - margins.right
    let height = canvasHeight - margins.top - margins.bottom;

    let filteredData = data;


    // get x and y scales
    console.log("WIDTH")
    console.log(width)

    const x = d3.scaleTime().range([0, width]);
    //const y = d3.scaleLinear().range([height, 0]);
    
    x.domain(d3.extent(filteredData, d => new Date(d[xVal])));
    //y.domain([0, d3.max(data, d => d[yVal])]);

    // brushing
    let brushing = false;
    let brushStartX = 0;
    let brushEndX = 0;

    canvas.removeEventListener('mousedown', mouseDown);
    canvas.addEventListener('mousedown', mouseDown);
    canvas.removeEventListener('mousemove', mouseMove);
    canvas.addEventListener('mousemove', mouseMove);
    canvas.removeEventListener('mouseup', mouseUp);
    canvas.addEventListener('mouseup', mouseUp);

    function mouseDown(event) {
        console.log(event)
        canvas.style.cursor = 'grabbing';
        brushing = true;
        brushStartX = event.clientX - canvas.getBoundingClientRect().left - margins.left;
        brushEndX = brushStartX;
        startDate = x.invert(brushStartX);
        
    }

    function mouseMove(event) {
        if (!brushing) return;
        brushEndX = event.clientX - canvas.getBoundingClientRect().left - margins.left;
        endDate = x.invert(brushEndX);
        // draw the brush
        context.fillStyle = 'rgba(0, 0, 100, 0.01)';
        context.fillRect(brushStartX, 0, brushEndX - brushStartX, canvasHeight);
    }
    
    function mouseUp(event) {
        console.log(event)
        canvas.style.cursor = 'auto';
        if (!brushing) return;
        brushing = false;
        brushEndX = event.clientX - canvas.getBoundingClientRect().left - margins.left;

        startpx = d3.min([brushStartX, brushEndX])
        endpx = d3.max([brushStartX, brushEndX])

        startDate = x.invert(startpx);
        endDate = x.invert(endpx);
        
        filteredData = data.filter(item => {
            return ( (new Date(item[xVal]) > startDate) & (new Date(item[xVal]) < endDate ) ) ;
        })

        const paramInfo = getParam();
        createPlot(filteredData, xVal, paramInfo.paramName, canvasID, canvasWidth, canvasHeight, margins, yAxisLabel = paramInfo.paramLabel);

        // here you might want to update your graph based on the brush extents
    }

    // function updatePlot(data, xVal, yVal, canvasId, canvasWidth, canvasHeight, margins) {
    //     // clear the canvas and redraw everything
    //     context.clearRect(-margins.left, -margins.top, canvas.width, canvas.height);
    //     context.save();
    //     context.translate(margins.left, margins.top);
    //     createPlot(data, xVal, yVal, canvasId, canvasWidth, canvasHeight, margins);
    //     context.restore();

        
    // }
    
    // function drawBrush(){
    //     context.translate(margins.left, margins.top);
    //     // draw the brush
    //     context.fillStyle = 'rgba(0, 0, 0, 0.1)';
    //     context.fillRect(brushStartX, height, brushEndX - brushStartX, margins.bottom);
    // }
}


// very specific to this current app
// i think the dataset should maybe just have the actual column name in it. I forgot why it doesnt..
function getParam(){
    const activeButton = document.querySelector('.logger-visual-tab-button.active');
    return {
        paramName: `raw_${activeButton.dataset.parameter}`,
        paramLabel: activeButton.dataset.parameterLabel
    }
}