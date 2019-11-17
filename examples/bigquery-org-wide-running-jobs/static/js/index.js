$(document).ready(function () {
	$('#job-table').DataTable( {
		"ajax": {
			"url": "https://festive-terrain-1.appspot.com/_ah/get-handlers/v1/jobs",
			"complete": function() {
				jobIdOnClick();
			  }
		},
        "columns": [
			{ "data": "jobid",
              'createdCell':  function (td, cellData, rowData, row, col) {
				  $(td).html('<a class="jobID" data-jobid=' + cellData + ">" + cellData + '</a>');
				  // either add onclick to the element 
				  // or call back async or wait
                }
			},
			{ "data": "email" },
			{ "data": "projectid" },
			{ "data": "reservationid" },
			{ "data": "slots" },
			{ "data": "state" },
		]
	} );
	
} );

function jobIdOnClick() {
	alert("called function a");
	var a = document.getElementsByClassName("jobID");
    for (i = 0; i < a.length; i++){
        a[i].onclick = function(e) {
			// call https://festive-terrain-1.appspot.com/_ah/get-handlers/v1/jobid/{jobid}
			const data = {"createtime":"2019-11-15T03:40:03.834Z","starttime":"2019-11-15T03:40:04.214Z","endtime":"2019-11-15T03:41:42.095Z","projectid":"\"festive-terrain-1\"","jobid":"\"bquxjob_32a27f2f_16e6d256f12\"","location":"\"US\"","activeunits":[1595,2729,2736,2737,2746,2639,1747,1749,1748,1742,1721,1702,1673,1663,1649,1648,1648,1646,1640,1635,1618,1603,1569,1527,1473,1414,1327,1254,1146,1025,890,753,611,509,410,350,360,312,264,194,158,136,126,108,100,90,70,38,20,10,2,127,253,425,639,1753,2900,3952,4781,5586,6036,6921,7153,7014,6954,6295,5984,2099,750,46,34,32,12,0,0,0],"completedunits":[2,2,2,2,2,1002,1002,1002,1003,1016,1038,1062,1087,1099,1103,1105,1108,1111,1116,1129,1143,1171,1204,1255,1319,1387,1457,1547,1656,1784,1919,2045,2173,2272,2381,2452,2538,2573,2597,2625,2640,2649,2652,2661,2666,2670,2689,2697,2705,2711,2712,2713,2713,2713,2714,2752,2835,2930,3092,3616,4340,4577,5404,5897,6346,6625,8955,11044,12686,12693,12696,12703,12712,12713,12713,12713],"pendingunits":[1600,2711,2711,2711,2711,1711,1711,1711,1710,1697,1675,1651,1626,1614,1610,1608,1605,1602,1597,1584,1570,1542,1509,1458,1394,1326,1256,1166,1057,929,794,668,540,441,332,261,175,140,116,88,73,64,61,52,47,43,24,16,8,2,1,10000,10000,10000,9999,9961,9878,9783,9621,9097,8373,8136,7309,6816,6367,6088,3758,1669,27,20,17,10,1,0,0,0],"elapsed":[735000000,1743000000,2865000000,4238000000,6177000000,7346000000,7851000000,10031000000,11176000000,12782000000,13837000000,14901000000,16075000000,17259000000,18641000000,19704000000,22250000000,23589000000,24933000000,26039000000,27130000000,28263000000,29301000000,30388000000,31394000000,32419000000,33446000000,34492000000,35502000000,36530000000,37546000000,38556000000,39573000000,40612000000,41626000000,42651000000,44162000000,45233000000,46303000000,47374000000,48661000000,49736000000,50988000000,52160000000,53277000000,54355000000,56492000000,57933000000,59113000000,61117000000,66162000000,68778000000,69815000000,70838000000,71842000000,72846000000,73854000000,74860000000,75865000000,76867000000,77869000000,78876000000,79882000000,80899000000,81939000000,82439000000,84320000000,87030000000,88038000000,89144000000,89786000000,91379000000,92435000000,93118000000,95552000000,97782000000],"type":"\"Query\"","state":"\"Done\"","error":"\"\"","email":"\"\"","src":"\"bigquery-public-data:austin_311.311_request,bigquery-public-data:austin_incidents.incidents_2016\"","dst":"\"festive-terrain-1:_b58ac6f2ae178e2f175a74e0ed5d5869c4deb2ac.anon5d629fb8a8dd77148fba186c0eb4105af9845e36\"","priority":"\"INTERACTIVE\"","statementtype":"\"SELECT\"","query":"\"select descript from `bigquery-public-data.austin_incidents.incidents_2016`\\ncross join `bigquery-public-data.austin_311.311_request`\\n\"","updated":"2019-11-15T03:41:42.489124Z","reservationid":"\"reservation_0\"","slots":50};
			console.log("click");
			console.log(e.srcElement.attributes.getNamedItem("data-jobid").value);
			console.log(data);
			$(".modal").addClass("is-active");
            drawChartLine(data["activeunits"], data["completedunits"], data["pendingunits"], data["elapsed"]);
	    };
	}

	$( "#modal-close" ).click(function() {
		$(".modal").removeClass("is-active");
	});
	$( "#modal-close1" ).click(function() {
		$(".modal").removeClass("is-active");
	});
}

var isLive = false;
var interval;
var d1h = false;
$( "#livebutton" ).click(function() {
	if (!isLive) {
		$("#livestatus").addClass("has-text-danger");
		isLive = true;
		interval = setInterval(function() {
			var today = new Date();
			var date = today.getFullYear()+'-'+(today.getMonth()+1)+'-'+today.getDate()+ ' ' + today.getHours() + ':' + today.getMinutes() + ':' + today.getSeconds();
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
$( "#d1h" ).click(function() {
	
	if (!d1h) {
		$("#d1h").addClass("has-background-info has-text-white");
		d1h = true;
	} else {
		$("#d1h").removeClass("has-background-info has-text-white");
		d1h = false;
	}
});

google.charts.load('current', {'packages':['treemap']});
google.charts.setOnLoadCallback(drawChart);

function drawChart() {
	var data = google.visualization.arrayToDataTable([
		['Location', 'Parent', 'Market trade volume (size)', 'Market increase/decrease (color)'],
		['pac/thd/',   		 null,                 0,                               0],
		['pac/thd/pr',   	'pac/thd/',          0,                               0],
		['pac/thd/io',    	'pac/thd/',             0,                               0],
		['pac/thd/npr',     'pac/thd/',      0,                               0],
		['pac/thd/sc', 		'pac/thd/',             0,                               0],
		['pac/thd/st',    	'pac/thd/',             0,                               0],
		['pac/thd/us',   	'pac/thd/pr',           11,                              10],
		['pac/thd/cn',      'pac/thd/pr',           52,                              31],
		['pac/thd/eu',    	'pac/thd/pr',           24,                              12],
		['pac/thd/pr/sc',   'pac/thd/pr',           16,                             -23],
		['pac/thd/eu/adhoc','pac/thd/eu',             42,                              -11],
		['pac/thd/eu/sla',  'pac/thd/eu',             31,                              -2],
		['pac/thd/eu/nosla','pac/thd/eu',             22,                              -13],
		['pac/thd/eu/dev',  'pac/thd/eu',             17,                              4],
		['pac/thd/eu/test', 'pac/thd/eu',             21,                              -5],
		['pac/thd/npr/sla', 'pac/thd/npr',               36,                              4],
		['pac/thd/npr/nosla','pac/thd/npr',               20,                              -12],
		['pac/thd/npr/prod','pac/thd/npr',               40,                              63],
		['pac/thd/npr/dev', 'pac/thd/npr',               4,                               34],
		['pac/thd/npr/test','pac/thd/npr',               1,                               -5],
		['pac/thd/npr/uat', 'pac/thd/npr',               12,                              24],
		['pac/thd/npr/io1', 'pac/thd/npr',               18,                              13],
		['Pakistan',  		'pac/thd/npr',               11,                              -52],
		['pac/thd/sc/npa',  'pac/thd/sc',             21,                              0],
		['pac/thd/sla', 	'pac/thd/sc',             30,                              43],
		['pac/thd/nosla',   'pac/thd/sc',             12,                              2],
		['pac/thd/dev',     'pac/thd/sc',             10,                              12],
		['pac/thd/test',    'pac/thd/sc',             8,                               10]
		]);

	
	tree = new google.visualization.TreeMap(document.getElementById('chart_div'));
	tree.draw(data, {
		minColor: '#007000',
		midColor: '#FFBF00',
		maxColor: '#D2222D',
		headerHeight: 25,
		fontColor: 'black'
		,showScale: true
	});

	google.visualization.events.addListener(tree, 'select', function(){
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

google.charts.load('current', {'packages':['corechart']});
google.charts.setOnLoadCallback(drawChartLine);

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
		chartArea: {'width': '70%', 'height': '70%'},
		// Gives each series an axis that matches the vAxes number below.
		series: {
			0: {targetAxisIndex: 0},
			2: {targetAxisIndex: 2}
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