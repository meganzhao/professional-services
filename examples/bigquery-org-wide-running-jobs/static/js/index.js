

	$(document).ready(function() {
		$('#job-table').DataTable( {
			"ajax": {
				"url": "https://festive-terrain-1.appspot.com/_ah/get-handlers/v1/jobs",
			},
			"columns": [
                { "data": "jobid"},
				{ "data": "email" },
				{ "data": "projectid" },
				{ "data": "reservationid" },
				{ "data": "slots" },
				{ "data": "state" },
			]
		} );
	} );
		

		$( "#bq-job-123dfdh9sd9sdf7sd" ).click(function() {
			$(".modal").addClass("is-active");
			drawChartLine();
		});
		$( "#modal-close" ).click(function() {
			$(".modal").removeClass("is-active");
		});
		$( "#modal-close1" ).click(function() {
			$(".modal").removeClass("is-active");
		});
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

      	function drawChartLine() {
        	var data = google.visualization.arrayToDataTable([
	          	['Time', 'Pending Units', 'Total Units', 'Total Slots'],
	          	['10:01:01',  4000,      4000, 100],
	          	['10:01:02',  3900,      4000, 120],
	          	['10:01:03',  3800,      4000, 120],
	          	['10:01:04',  3200,      4000, 130],
	          	['10:01:05',  2800,      4000, 140],
	          	['10:01:06',  2700,      4000, 100],
	          	['10:01:07',  2600,      4000, 80],
	          	['10:01:08',  2500,      4000, 120],
	          	['10:01:09',  1400,      4000, 140],
	          	['10:01:10',  1390,      4000, 140],
	          	['10:01:11',  1380,      4000, 200],
	          	['10:01:12',  1290,      4000, 60],
	          	['10:01:13',  1270,      4000, 70],
	          	['10:01:14',  1250,      4000, 70],
	          	['10:01:15',  1180,      4000, 80],
	          	['10:01:16',  1150,      4000, 70],
	          	['10:01:17',  1100,      4000, 120],
	          	['10:01:18',  1080,      4000, 130],
	          	['10:01:19',  1070,      4000, 140],
	          	['10:01:20',  1060,      4000, 150],
	          	['10:01:21',  1050,      4000, 160],
	          	['10:01:22',  1040,      4000, 30],
	          	['10:01:23',  1030,      4000, 40],
	          	['10:01:24',  1030,      4000, 50],
	          	['10:01:25',  1030,      4000, 60],
	          	['10:01:26',  1030,      4000, 120],
        		]);
        	
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