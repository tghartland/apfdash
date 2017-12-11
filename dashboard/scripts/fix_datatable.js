$(document).ready(function() {

    function check_for_button() {
        // check if toolbar element exists yet
        if ($('div.react-grid-Toolbar .tools button.btn').length) {
            // activate filter button
            $('div.react-grid-Toolbar .tools button.btn').click();

            // and hide the toolbar
            $('div.react-grid-Toolbar').hide();
        } else {
            // check again in 100ms
            setTimeout(check_for_button, 100);
        }
    }

    check_for_button();
});