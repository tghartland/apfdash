// Click the datatable filters button to have it enabled by default.
$.initialize("div.react-grid-Toolbar .tools button.btn", function() {
	$(this).click();
});

// And hide the datatable toolbar altogether.
$.initialize("div.react-grid-Toolbar", function() {
	$(this).hide();
});

// Hide the dash undo/redo buttons, as they float over some of the plots.
$.initialize("div._dash-undo-redo", function() {
    $(this).hide();
});

// Make the queue names in the datatable into links.
// The first check is to select cells only from
// the leftmost column.
$.initialize("div.react-grid-Cell .react-grid-Cell__value div span div", function() {
    if ($(this)[0].parentElement.parentElement.parentElement.parentElement.style.left == "0px") {
        $(this)[0].onclick = function () {
            window.history.pushState({}, '', "/queue/" + $(this)[0].title);
            window.dispatchEvent(new Event('onpushstate'));
            return false;
        };
    }
});

// Overwrite bar chart label links' click handlers.
// This changes the behaviour from open in new tab
// to open in same window, to be consistent with
// Dash style links.
$.initialize("text a", function() {
    $(this)[0].onclick = function () {
        window.history.pushState({}, '', $(this)[0].href.baseVal);
        window.dispatchEvent(new Event('onpushstate'));
        return false;
    };
});