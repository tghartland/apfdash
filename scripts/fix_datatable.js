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




// Resizes columns to distribute extra width to queue name column
// Not using this because the change isn't permanent - when the browser
// is resized the columns revert to their original size.

/*function tableResize() {
    width = parseInt($(this)[0].style.width, 10);
    left = parseInt($(this)[0].style.left, 10);
    index = left/width;
    change = 0.15;
    if (index === 0) {
        $(this)[0].style.width = (width*(1+6*change)) + "px";
    } else {
        $(this)[0].style.left = (width*(1+6*change) + width*(1-change)*(index-1)) + "px";
        $(this)[0].style.width = (width*(1-change)) + "px";
    }
}

function tableResize2(cells) {
    for(var c=0; c<cells.length; c++) {
        var cell = cells[c];
        width = parseInt($(cell)[0].style.width, 10);
        left = parseInt($(cell)[0].style.left, 10);
        index = left/width;
        change = 0.15;
        if (index === 0) {
            $(cell)[0].style.width = (width*(1+6*change)) + "px";
        } else {
            $(cell)[0].style.left = (width*(1+6*change) + width*(1-change)*(index-1)) + "px";
            $(cell)[0].style.width = (width*(1-change)) + "px";
        }
    }
}

$.initialize(".react-grid-HeaderCell, .react-grid-Cell", tableResize);

function tableOnResize() {
    tableResize2($(".react-grid-HeaderCell, .react-grid-Cell"));
    $(window).resize(tableOnResize);
}

$(document).ready(function() {
    window.resize(tableOnResize);
});
*/