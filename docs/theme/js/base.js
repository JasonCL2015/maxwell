
/* Highlight */
$( document ).ready(function() {
    $('table').addClass('table table-striped table-hover');
});


$('body').scrollspy({
    target: '.bs-sidebar',
		offset: 100
});


/* Prevent disabled links from causing a page reload */
$("li.disabled a").click(function() {
    event.preventDefault();
});



