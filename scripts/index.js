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
// style*='left: 0px' selects only the reacts grid cells
// that are in the leftmost column (queue names).
$.initialize("div.react-grid-Cell[style*='left: 0px'] .react-grid-Cell__value div span div", function() {
    $(this)[0].onclick = function () {
        window.history.pushState({}, '', URL_PREFIX + "queue/" + $(this)[0].title);
        window.dispatchEvent(new Event('onpushstate'));
        return false;
    };
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


// Add examples to data table's search box
// :eq(1) selects only the second search box
$.initialize(".react-grid-HeaderCell div div input:gt(0)", function() {
    $(this)[0].setAttribute("placeholder", "Filter");
});
$.initialize(".react-grid-HeaderCell div div input:eq(2)", function() {
    $(this)[0].setAttribute("placeholder", "Filter (e.g >5000)");
});
$.initialize(".react-grid-HeaderCell div div input:eq(3)", function() {
    $(this)[0].setAttribute("placeholder", "Filter (e.g <20)");
});

$.initialize("#help-panel-collapsing-link", function() {
    $(this)[0].setAttribute("data-toggle", "collapse");
    $(this)[0].setAttribute("href", "#collapsehelp")
});
