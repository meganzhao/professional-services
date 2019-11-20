$(document).ready(function () {
	$.ajax({ 
		type: 'GET', 
		url: 'https://festive-terrain-1.appspot.com/_ah/get-handlers/v1/jobs', 
		data: { get_param: 'value' }, 
		dataType: 'json',
		success: function (data) { 
			 // Load the Visualization API and the package and
			 // set a callback to run when the Google Visualization API is loaded.
			google.charts.load('current', {
				'callback': function() {
					drawChart(data);
					jobList(data);
				},
				'packages': ['treemap', 'corechart'] });
		}
	});
});

function jobList(data) {
	$('#job-table').DataTable({
		// TODO: get data from variable
		"data": data["data"],
		"columns": [
			{
				"data": "jobid",
				'createdCell': function (td, cellData, rowData, row, col) {
					$(td).html('<a>' + cellData + '</a>');
					$(td).click(
						function () {
							$(".modal").addClass("is-active");
							drawChartLine(rowData["activeunits"], rowData["completedunits"], rowData["pendingunits"], rowData["elapsed"]);
						}
					);
				}
			},
			{ "data": "email" },
			{ "data": "projectid" },
			{ "data": "reservationid" },
			{ "data": "slots" },
			{ "data": "state" },
		]
	});
	$("#modal-close").click(function () {
		$(".modal").removeClass("is-active");
	});
	$("#modal-close1").click(function () {
		$(".modal").removeClass("is-active");
	});
}

var isLive = false;
var interval;
var d1h = false;
$("#livebutton").click(function () {
	if (!isLive) {
		$("#livestatus").addClass("has-text-danger");
		isLive = true;
		interval = setInterval(function () {
			var today = new Date();
			var date = today.getFullYear() + '-' + (today.getMonth() + 1) + '-' + today.getDate() + ' ' + today.getHours() + ':' + today.getMinutes() + ':' + today.getSeconds();
			//$("#currenttime").textText=date;
			document.getElementById('currenttime').innerHTML = date;
			//$("#currenttime").load(date);
		}, 1000);
	} else {
		$("#livestatus").removeClass("has-text-danger");
		isLive = false;
		clearInterval(interval);
	}
});
$("#d1h").click(function () {

	if (!d1h) {
		$("#d1h").addClass("has-background-info has-text-white");
		d1h = true;
	} else {
		$("#d1h").removeClass("has-background-info has-text-white");
		d1h = false;
	}
});

function sum(arr, key) {
	var total = 0;
	for (var i = 0; i < arr.length; i++) {
		row = arr[i];
		total += row[key];
	}
	return total;
}

function groupBy(xs, key) {
	return xs.reduce(function (rv, x) {
		(rv[x[key]] = rv[x[key]] || []).push(x);
		return rv;
	}, {});
}

function reservationUsage(jsonData) {
	// initialization
	// create a json obj to store slots?
	var arr = [];
	arr.push(['Location', 'Parent', 'Market trade volume (size)', 'Market increase/decrease (color)']);
	arr.push(["all", null, 0, 0]);

	console.log(jsonData);

	// group by reservation id
	var groupbyReservationId = groupBy(jsonData["data"], "reservationid");
	console.log(groupbyReservationId);
	for (var reservationId in groupbyReservationId) {
		var slotsbyReservation = sum(groupbyReservationId[reservationId], "slots")
		arr.push([reservationId, "all", slotsbyReservation, 0]);

		var slotsbyProject = slotsbyReservation / groupbyReservationId[reservationId].length;
		var groupbyProject = groupBy(groupbyReservationId[reservationId], 'projectid');
		for (var projectId in groupbyProject) {
			arr.push([projectId, reservationId, slotsbyProject, 0]);

			var slotsbyUser = slotsbyProject / groupbyProject[projectId].length;
			var groupbyUser = groupBy(groupbyProject[projectId], 'email');
			for (var email in groupbyUser) {
				arr.push([email, projectId, slotsbyUser, 0]);
			}
		}
		console.log(arr);
	}
	return arr;
}

function drawChart(jsonData) {
	var data = google.visualization.arrayToDataTable(reservationUsage(jsonData));
	tree = new google.visualization.TreeMap(document.getElementById('chart_div'));
	tree.draw(data, {
		minColor: '#007000',
		midColor: '#FFBF00',
		maxColor: '#D2222D',
		headerHeight: 25,
		fontColor: 'black'
		, showScale: true
	});

	google.visualization.events.addListener(tree, 'select', function () {
		var selectedItem = tree.getSelection()[0];
		var size = 10;
		var value = 20;
		if (selectedItem) {
			var row = selectedItem.row;
			//alert('The user selected ' + value);
			/*
			alert(
	'' + data.getValue(row, 0) +
	', ' + data.getValue(row, 1) + 
	', ' + data.getValue(row, 2) +
	', ' + data.getValue(row, 3) + 
	', ' +
	'Datatable row: ' + row + 
	', ' + data.getColumnLabel(2) +
	' (total value of this cell and its children): ' + size + 
	', ' + data.getColumnLabel(3) + 
	': ' + value );
	*/
		}

	});
}

function drawChartLine(activeunits, completedunits, pendingunits, elapsed) {
	// TODO: cut all arrays to last 25

	var dataArray = [['elapsed', 'activeunits', 'completedunits', 'pendingunits']];

	if (activeunits === undefined || completedunits === undefined ||
		pendingunits === undefined || elapsed === undefined) {
		return;
	}

	for (var n = 0; n < activeunits.length; n++) {
		dataArray.push([elapsed[n], activeunits[n], completedunits[n], pendingunits[n]]);
	}
	console.log(dataArray);

	var data = new google.visualization.arrayToDataTable(dataArray);

	// var data = google.visualization.arrayToDataTable([
	// 	['Time', 'Pending Units', 'Total Units', 'Total Slots'],
	// 	['10:01:01',  4000,      4000, 100],
	// 	['10:01:02',  3900,      4000, 120],
	// 	]);

	var options = {
		pointSize: 2,
		curveType: 'function',
		legend: 'top',
		height: 600,
		width: 600,
		chartArea: { 'width': '70%', 'height': '70%' },
		// Gives each series an axis that matches the vAxes number below.
		series: {
			0: { targetAxisIndex: 0 },
			2: { targetAxisIndex: 2 }
		},
		hAxis: {
			title: 'Timeline',
			slantedText: true,
			gridlines: {
				count: 11
			}
		},
		vAxis: {
			// Adds titles to each axis.
			0: {
				title: 'Work Units'
			},
			2: {
				title: 'Slots utilized'
			}
		}
	};

	var chart = new google.visualization.LineChart(document.getElementById('curve_chart'));

	chart.draw(data, options);
}