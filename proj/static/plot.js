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
        const acceptableDataTypes = ['Numeric','Time','Categorical'];
        console.assert(acceptableDataTypes.includes(dtype), `Error in setting datatype in xDataType - must be ${acceptableDataTypes.join(', ')}`)
        this._xDataType = dtype;
    }
   
    get xDataType() {
        return this._xDataType;
    }
    
    set yDataType(dtype) {
        const acceptableDataTypes = ['Numeric','Time','Categorical'];
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

    set visHeight (height) {
        this._visHeight = height;
    }
    set visWidth (width) {
        this._visWidth = width;
    }
    get visHeight () {
        return this._visHeight ;
    }
    get visWidth () {
        return this._visWidth ;
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
    set xAxisTextRotation(n){
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
            .attr('id','plot-main-title')
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
                .style("font-size",`${xAxisFontSize}px`)
                .attr("dx", "-1em")
                .attr("dy", ".2em")
                .attr("transform", `rotate(${xAxisRotation})` );
                
        const yAxisGroup = this.chartArea.append("g")        
        yAxisGroup.call(d3.axisLeft(this.yScale))
            .selectAll("text")  
                .style("font-size",`${yAxisFontSize}px`)
                .attr("transform", `rotate(${yAxisRotation})` );

        this.svg.append('g')
            .attr('class','x-axis-label')
            .append('text')
            .attr('transform',`translate(${(vis._visWidth / 2) + vis.margins.left},${vis._canvasHeight - vis.margins.bottom / 5})`)
            .text(xAxisLabel)
        
        this.svg.append('g')
            .attr('class','y-axis-label')
            .append('text')
            .attr('text-anchor','middle')
            .attr('transform',`rotate(-90) translate(${-(vis.margins.top + (vis._visHeight / 2))}, ${(vis.margins.left * 0.3)})`)
            .text(yAxisLabel)
    }

    setScale(x, y, xDataType, yDataType, xScaleType = 'Linear', yScaleType = 'Linear', padding = 0.2) {
        const acceptableDataTypes = ['Numeric','Time','Categorical'];
        console.assert(acceptableDataTypes.includes(xDataType), `Error in use of PathPlot class draw method - xDataType must be ${acceptableDataTypes.join(', ')}`)
        console.assert(acceptableDataTypes.includes(yDataType), `Error in use of PathPlot class draw method - yDataType must be ${acceptableDataTypes.join(', ')}`)

        if (xDataType == 'Time') {
            this.xScale = d3.scaleTime()
                .domain([d3.min(this.filteredData, d => d[x]), d3.max(this.filteredData, d => d[x])])
                .range([ 0, this._visWidth ])
        }
        if (yDataType == 'Time') {
            this.yScale = d3.scaleTime()
                .domain([d3.min(this.filteredData, d => d[y]), d3.max(this.filteredData, d => d[y])])
                .range([ 0, this._visHeight ])
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
        if (!this._canvasHeight){
            console.warn("Warning in use of Plot class - canvas height not defined")
            this._canvasHeight = document.getElementById(this.chartID).getBoundingClientRect().height;
        }
        if (!this._canvasWidth){
            console.warn("Warning in use of Plot class - canvas width not defined")
            this._canvasWidth = document.getElementById(this.chartID).getBoundingClientRect().width;
        }
        if (!this._margins){
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
            .style('background-color','white')

        this.chartArea = this.svg.append('g')
            .attr('width', this._visWidth)
            .attr('height',this._visHeight)
            .attr('transform',`translate(${this._margins.left},${this._margins.top})`)
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

        console.assert(this._margins,  "Error in use of PathPlot - Margins are not defined but an attempt to draw was made. Did you call the init() method?")
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
            {xAxisLabel : xAxisLabel, 
            yAxisLabel : yAxisLabel}
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
