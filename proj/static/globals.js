const critical_error_handler = (contact) => {
    alert(
`
The Application suffered an internal server error. 
The maintainers of the application have been notified. 
Please feel free to contact ${contact} for assistance.
`
    )
    window.location = `/${script_root}`
};

const LOGGER_DATA_VISUAL_PARAMS = [
    {
        paramName:'do',
        paramLabel: 'Dissolved Oxygen'
    },
    {
        paramName:'do_pct',
        paramLabel: 'Dissolved Oxygen (%)'
    },
    {
        paramName:'h2otemp',
        paramLabel: 'Water Temperature'
    },
    {
        paramName:'turbidity',
        paramLabel: 'Turbidity'
    },
    {
        paramName:'salinity',
        paramLabel: 'Salinity'
    },
    {
        paramName:'pressure',
        paramLabel: 'Pressure'
    },
    {
        paramName:'chlorophyll',
        paramLabel: 'Chlorophyll'
    }
]