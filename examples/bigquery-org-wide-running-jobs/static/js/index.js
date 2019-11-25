$(document).ready(function () {
	$.ajax({
		type: 'GET',
		url: 'https://festive-terrain-1.appspot.com/_ah/get-handlers/v1/jobs',
		data: { get_param: 'value' },
		dataType: 'json',
		success: function (data) {
			data = data["data"];
			// calculat slot usage
			for (i in data) {
				rowData = data[i]
				const length = rowData["activeunits"].length
				rowData["slotUsage"] = new Array(length);

				for (j = 0; j < length; j++) {
					rowData["slotUsage"][j] = rowData["slotmillis"][j] * 1000000 / rowData["elapsed"][j];
				}
			}
			// Load the Visualization API and the package and
			// set a callback to run when the Google Visualization API is loaded.
			google.charts.load('current', {
				'callback': function () {
					drawReservationChart(data);
					jobList(data);
				},
				'packages': ['treemap', 'corechart']
			});
		}
	});
});



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




function drawReservationChart(jsonData) {
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

// reservation usage section
function reservationUsage(jsonData) {
	// initialization
	var arr = [];
	arr.push(['Level', 'Parent', 'Reserved slot (size)', 'Slot usage (color)']);
	arr.push(["all", null, 0, 0]);

	// group by reservation id
	var groupbyReservationId = groupBy(jsonData, "reservationid");
	for (var reservationId in groupbyReservationId) {
		// var slotsbyReservation = sum(groupbyReservationId[reservationId], "slots");
		var slotsbyReservation = findSlot(jsonData, reservationId);
		var slotUsagebyReservation = 0;

		var groupbyProject = groupBy(groupbyReservationId[reservationId], 'projectid');
		var slotsbyProject = slotsbyReservation / Object.keys(groupbyProject).length;

		for (var projectId in groupbyProject) {
			var groupbyUser = groupBy(groupbyProject[projectId], 'useremail');
			var slotsbyUser = slotsbyProject / Object.keys(groupbyUser).length;
			var slotUsagebyProject = 0;

			for (var email in groupbyUser) {
				var slotUsagebyUser = sum(groupbyUser[email], "slotUsage");
				slotUsagebyProject += slotUsagebyUser;
				slotUsagebyUser /= slotsbyUser;
				arr.push([{v: projectId + "/" + email, f: email + 
					" (Reserved slots: " + slotsbyUser.toFixed(2) + "; slotUsage: " + 
					slotUsagebyUser.toFixed(2) + "%; number of jobs: " + 
					groupbyUser[email].length + ")"}, 
					projectId, slotsbyUser, slotUsagebyUser]);
			}			
			
			arr.push([{v: projectId, f: projectId + 
				" (Reserved slots: " + slotsbyProject.toFixed(2) + "; slotUsage: " + 
				(slotUsagebyProject / slotsbyProject).toFixed(2) + "%; number of users: " + 
				Object.keys(groupbyUser).length + ")"}, 
				reservationId, 0, 0]);

			slotUsagebyReservation += slotUsagebyProject;
		}

		arr.push([{v: reservationId, f: reservationId + 
			" (Reserved slots: " + slotsbyReservation.toFixed(2) + "; slotUsage: " + 
			(slotUsagebyReservation / slotsbyReservation).toFixed(2) + "%; number of projects: " + 
			Object.keys(groupbyProject).length + ")"}, 
			"all", 0, 0]);
	}
	return arr;
}

//helper function for reservationUsage
function sum(arr, key) {
	var total = 0;
	if (key == "slotUsage") {
		for (var i = 0; i < arr.length; i++) {
			console.log(arr);
			row = arr[i];
			// add the last slot average usage to total
			total += row[key][row[key].length - 1];
		}
	} else {
		for (var i = 0; i < arr.length; i++) {
			row = arr[i];
			total += row[key];
		}
	}
	return total;
}

//helper function for reservationUsage
function groupBy(xs, key) {
	return xs.reduce(function (rv, x) {
		(rv[x[key]] = rv[x[key]] || []).push(x);
		return rv;
	}, {});
}

function findSlot(data, reservationId) {
	for (var i = 0; i < data.length; i++) {
		row = data[i];
		if (row["reservationid"] == reservationId) {
			return row["slots"];
		}
	}
}

// job list section
function jobList(data) {
	$('#job-table').DataTable({
		// TODO: get data from variable
		"data": data,
		"columns": [
			{
				"data": "jobid",
				'createdCell': function (td, cellData, rowData, row, col) {
					$(td).html('<a>' + cellData + '</a>');
					$(td).click(
						function () {
							$(".modal").addClass("is-active");
							drawChartLine(rowData);
						}
					);
				}
			},
			{ "data": "useremail" },
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

function drawChartLine(rowData) {
	var activeunits = rowData["activeunits"]
		, completedunits = rowData["completedunits"]
		, pendingunits = rowData["pendingunits"]
		, slotUsage = rowData["slotUsage"]
		, elapsed = rowData["elapsed"]
		, starttime = new Date(rowData["starttime"])
		, jobId = rowData["jobid"]
		, projectId = rowData["projectid"]
		, query = rowData["query"];


	const length = rowData["activeunits"].length
	// number of milliseconds since 1 January 1970 00:00:00
	starttime = starttime.getTime();

	for (i = 0; i < length; i++) {
		// 1 milliseconds = 1000000 Nanoseconds 
		rowData["elapsed"][i] = new Date(starttime + rowData["elapsed"][i] / 1000000)
	}

	var dataArray = [['elapsed', 'activeunits', 'pendingunits', 'completedunits', 'slotUsage']];

	if (activeunits === undefined || slotUsage === undefined ||
		pendingunits === undefined || completedunits === undefined ||
		elapsed === undefined) {
		return;
	}

	for (var n = 0; n < activeunits.length; n++) {
		dataArray.push([elapsed[n], activeunits[n], pendingunits[n], completedunits[n], slotUsage[n]]);
	}

	var data = new google.visualization.arrayToDataTable(dataArray);

	var options = {
		title: "JobId: " + jobId + "\n" +
			"ProjectId: " + projectId + "\n" +
			"Query: " + query + "\n",
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
			title: 'Timeline (UTC-6)',
			format: 'M/d/yy hh:mm:ss',
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