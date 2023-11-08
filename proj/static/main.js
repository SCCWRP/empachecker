
(function(){
    
    // const loginForm = document.getElementById("main-login-form");
    const loginForms = document.getElementsByClassName("login-form");
    const fileForm = document.getElementById("file-submission-form");

    Array.from(loginForms).forEach(loginForm => {
        //routine for when the user logs in
        loginForm.addEventListener("submit", async function(e) {
            e.preventDefault();
            
            if (loginForm.querySelector("input[name='login_email']").value === '') {
                alert("Please enter an email address");
                return;
            }

            let missingFields = [];
            Array.from(loginForm.querySelectorAll('.login-form-element')).forEach(elem => {
                if (elem.value === '' && elem.parentElement.classList.contains('hidden') === false ) {
                    let tmp = elem.getAttribute('name').trim().toLowerCase().replace('login_','').replace(/_/,' ').replace(/\w\S*/g, (w) => (w.replace(/^\w/, (c) => c.toUpperCase())));
                    missingFields.push(tmp)
                }
            })
            if (missingFields.length > 0) {
                alert(`The login form is missing required information: ${missingFields.join(', ')}`);
                return;
            }

            const formData = new FormData(this);
            const response = await fetch(`/${script_root}/login`, {
                method: 'post',
                body: formData
            });
            console.log(response);
            const result = await response.json();
            console.log(result);

            // handling the case where there was a critical error
            if (result.critical_error) {
                // critical_error_handler defined in globals.js
                critical_error_handler(result.contact)
            } else if (Object.keys(result).includes("user_error_msg")) {
                alert(result.user_error_msg);
                window.location = `/${script_root}`;
            }

            // With the session data now posted, if should display the file submission form
            // We will need to post some kind of button that allows them to clear session data and start fresh too
            window.location = `/${script_root}`;
            
            // we can possibly validate the email address on the python side and return a message in "result"
            // and handle the situation accordingly
            // document.querySelector("#login-outer-container").style.display = "none";
            // document.querySelector(".before-submit").classList.remove("hidden");

            

        })
    })
    

    // routine for submitting the file(s)
    fileForm?.addEventListener("submit", async function(e) {
        
        e.stopPropagation();
        e.preventDefault();

        // an example of how we can put a loader gif
        //document.querySelector(".records-display-inner-container").innerHTML = '<img src="/changerequest/static/loading.gif">';
        document.querySelector(".after-submit").classList.add("hidden");
        document.querySelector(".before-submit").classList.add("hidden");
        document.getElementById("loader-gif-container").classList.remove("hidden");
        
        const dropped_files = document.querySelector('[type=file]').files;
        const formData = new FormData();
        for(let i = 0; i < dropped_files.length; ++i){
            /* submit as array to as file array - otherwise will fail */
            formData.append('files[]', dropped_files[i]);
        }
        
        const response = await fetch(`/${script_root}/upload`, {
            method: 'post',
            body: formData
        });
        document.getElementById("loader-gif-container").classList.add("hidden");
        document.querySelector(".after-submit").classList.remove("hidden");
        const result = await response.json();

        // handling the case where there was a critical error
        if (result.critical_error) {
            // critical_error_handler defined in globals.js
            critical_error_handler(result.contact)
        } else if (Object.keys(result).includes("user_error_msg")) {
            alert(result.user_error_msg);
            window.location = `/${script_root}`;
        }

        //show the final submit buttin
        if (Object.keys(result).includes("errs")) {
            if (result['errs'].length == 0){
                document.querySelector("#final-submit-button-container").classList.remove("hidden");
                addFinalSubmitListener()
                if (result.warnings?.length > 0) {
                    // Cover the case where there are no errors but there are warnings
                    // Giving the user a final warning/final chance to check their warnings
                    document.getElementById('final-warning-container').classList.remove('hidden');
                } else {
                    document.getElementById('final-warning-container').classList.add('hidden');
                }

                // No errors, so be sure the errors tab header isnt flagging anything with the red alert aymbols
                document.getElementById('errors-report-header').classList.remove('error-alert');
                document.getElementById('errors-report-header').innerText = document.getElementById('errors-report-header').innerText.replace('❗ ','')

            } else {
                // display what needs to show to let them know they have issues with the file
                document.querySelector("#reload-button-container").classList.remove("hidden");
                document.getElementById('errors-report-header').classList.add('error-alert');
                document.getElementById('errors-report-header').innerText = `❗ Errors`;
                document.getElementById('errors-report-header').addEventListener('click', function(e) {
                    this.classList.remove('error-alert');
                    this.innerText = this.innerText.replace('❗ ','')
                });
                
                // In the case where there are errors, no matter what we want to cover up this div
                // This div is only to be shown in the event that they are ready for final submit, but they have warnings
                document.getElementById('final-warning-container').classList.add('hidden');
            }
        }
        if (Object.keys(result).includes("warnings") && (result['warnings'].length > 0) ) {
            document.getElementById('warnings-report-header').classList.add('warning-alert');
            document.getElementById('warnings-report-header').innerText = `⚠️ Warnings`;
            document.getElementById('warnings-report-header').addEventListener('click', function(e) {
                this.classList.remove('warning-alert');
                document.getElementById('warnings-report-header').innerText = document.getElementById('warnings-report-header').innerText.replace('⚠️ ','')
            });
        } else {
            // No warnings, so make sure that warnings alerting container is not showing
            document.getElementById('final-warning-container').classList.add('hidden');

            document.getElementById('warnings-report-header').classList.remove('warning-alert');
            document.getElementById('warnings-report-header').innerText = document.getElementById('warnings-report-header').innerText.replace('⚠️ ','')
        }

        buildReport(result);
        
        if (result.logger_data) {
            
            // prep the data
            // basically by only storing the data in a variable
            let loggerdata = result.logger_data

            // reset inner HTML for the button container before adding buttons
            document.getElementById('logger-visual-button-container').innerHTML = '';
            LOGGER_DATA_VISUAL_PARAMS.forEach((param, i) => {

                // dont append button if all data points are empty - most often loggers will not have all the parameters - only certain measurements were taken most times
                console.log("param")
                console.log(param)
                console.log(loggerdata.filter(d => d[`raw_${param.paramName}`] != ''))
                if (loggerdata.filter(d => d[`raw_${param.paramName}`] != '').length === 0) return;
                
                let units = `${[...new Set(loggerdata.map(d => d[`raw_${param.paramName}_unit`]))].join(',')}`
                
                // for the data-parameter-label attribute of the button
                units = units === '' ? units : ` (${units})` ; 

                console.log('units')
                console.log(units)
                btnHTML = `
                    <button 
                        id="${param.paramName}-tab-button" 
                        class="btn btn-primary logger-visual-tab-button ${i === 0 ? 'active' : ''}"
                        data-parameter="${param.paramName}"
                        data-parameter-label="${param.paramLabel}${units}"
                        data-bs-toggle="button"
                        ${i === 0 ? 'aria-pressed="true"' : ''}
                    >${param.paramLabel} 
                    </button>
                `
                document.getElementById('logger-visual-button-container').innerHTML += btnHTML;
            })
            
            // somewhat "global" variables
            const xAxisParameter = 'samplecollectiontimestamp';
            const chartID = 'logger-canvas';
            
            // plotWidth and plotHeight will be changing with resizing of the window
            let plotWidth = document.querySelector('.submission-report-outer-container').getBoundingClientRect().width * 0.9;
            let plotHeight = plotWidth * 0.75;

            // technically the resetPlot function should work also when drawing the plot for the first time
            resetPlot()



            // Add the event listeners as far as when to reset the plot
            window.addEventListener('resize', resetPlot)
            document.getElementById('reset-plot-button').addEventListener('click', resetPlot)

            Array.from(document.getElementsByClassName('logger-visual-tab-button')).forEach((btn, i, allButtons) => {
                btn.addEventListener('click', () => {
                    
                    // Do nothing if the clicked button is already the active one
                    if (btn.classList.contains('active')) return;

                    // otherwise change the clicked button to the active one
                    allButtons.forEach(b => {
                        b.classList.remove('active');
                        b.setAttribute('aria-pressed','false');
                    });

                    btn.classList.add('active');
                    btn.setAttribute('aria-pressed','true');
                    
                    resetPlot()
                })
            })


            // same code to reset the plot was showing up in multiple spots so i decided to put it in a function
            /* 
                using a function declaration rather than a function expression since function declarations are hoisted, 
                    so resetPlot will be found even though it is called above where this function is defined 
            */
            function resetPlot(){
                replaceCanvas();
                const activeButton = document.querySelector('.logger-visual-tab-button.active');
                // draw the new plot based on the active button
                let yVal = `raw_${activeButton.dataset.parameter}`;
                let plotWidth = document.querySelector('.submission-report-outer-container').getBoundingClientRect().width * 0.9;
                let plotHeight = plotWidth * 0.75;
                createPlot(
                    loggerdata, 
                    xAxisParameter, //xAxisParameter defined outside the function
                    yVal,
                    canvasId = chartID, //chartID defined outside the function
                    canvasWidth = plotWidth, 
                    canvasHeight = plotHeight, 
                    margins = {
                        top: plotHeight * 0.05, 
                        right: plotWidth * 0.02, 
                        bottom: plotHeight * 0.25, 
                        left: plotWidth * 0.10
                    },
                    yAxisLabel = activeButton.dataset.parameterLabel
                );
            }

            function replaceCanvas({canvasID = chartID} = {}){
                // Get the old canvas
                let oldCanvas = document.getElementById(canvasID);

                // Create a new canvas
                let newCanvas = document.createElement('canvas');

                // Copy the width and height attributes from the old canvas
                newCanvas.width = oldCanvas.width;
                newCanvas.height = oldCanvas.height;

                // Replace the old canvas with the new one
                oldCanvas.parentNode.replaceChild(newCanvas, oldCanvas);

                // Set the id of the new canvas to the id of the old canvas, if you need to keep it
                newCanvas.id = oldCanvas.id;

            }
        }
        
        // we can possibly validate the email address on the python side and return a message in "result"
        // and handle the situation accordingly
        // document.querySelector(".file-form-container").classList.add("hidden");

    })

    if (document.getElementById('clear-session-button')){
        document.getElementById('clear-session-button').addEventListener('click', async function(){
            const response = await fetch(`/${script_root}/`, {
                method: 'delete'
            });
            console.log(response);
            const result = await response.json();
            window.location = `/${script_root}/`;
        })
    }


})()