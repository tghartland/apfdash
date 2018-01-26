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
        window.history.pushState({}, '', "/queue/" + $(this)[0].title);
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
$.initialize(".react-grid-HeaderCell div div input:eq(1)", function() {
    $(this)[0].setAttribute("placeholder", "Search (e.g >5000)");
});