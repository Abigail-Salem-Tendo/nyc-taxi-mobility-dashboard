//Intializing frontend
const API_BASE = "http://127.0.0.1:5000"; 

// using dummy data as we await completion of some endpoints
const tripsByHour = [50, 80, 120, 200, 250, 300, 280, 260, 220, 180, 150, 100, 80, 60, 50, 40, 30, 20, 10, 5, 5, 5, 5, 5];
const avgSpeedByHour = [25, 23, 20, 18, 17, 16, 16, 17, 18, 19, 20, 22, 24, 25, 25, 26, 27, 28, 28, 29, 30, 30, 30, 30];
const farePerMileByHour = [2.5, 2.4, 2.3, 2.2, 2.1, 2.0, 2.0, 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.7, 2.8, 2.9, 3.0, 3.0, 3.1, 3.2, 3.2, 3.2, 3.2];



//  LOAD STATISTICS
async function loadStatistics() {
    const response = await fetch(`${API_BASE}/api/statistics`);
    const data = await response.json();

    const container = document.getElementById("statsContainer");
    container.innerHTML = "";

    const stats = [
        { label: "Total Trips", value: data.total_trips },
        { label: "Avg Speed (mph)", value: data.average_speed_in_milesperhour },
        { label: "Avg Fare per Mile", value: data.average_fare_per_mile },
        { label: "Avg Duration (min)", value: data.average_duration_min },
        { label: "Total Revenue ($)", value: data.total_fare_amount },
        { label: "Avg Distance (miles)", value: data.average_distance_miles },
        { label: "Avg Passengers", value: data.average_passengers },
        { label: "Avg Tip ($)", value: data.average_tip_amount }
    ];

    stats.forEach(stat => {
        const card = document.createElement("div");
        card.className = "stat-card";
        card.innerHTML = `<h3>${stat.label}</h3><p>${stat.value}</p>`;
        container.appendChild(card);
    });
}


// PICKUP ZONES
async function loadPickupZones() {
    const limit = document.getElementById("pickupLimit").value;
    const response = await fetch(`${API_BASE}/api/top-pickup-zones?limit=${limit}`);
    const result = await response.json();

    const tbody = document.querySelector("#pickupTable tbody");
    tbody.innerHTML = "";

    result.data.forEach(zone => {
        const row = `
            <tr>
                <td>${zone.rank}</td>
                <td>${zone.zone_name}</td>
                <td>${zone.trip_count}</td>
                <td>${zone.percentage}%</td>
            </tr>
        `;
        tbody.innerHTML += row;
    });
}


//  DROPOFF ZONES 
async function loadDropoffZones() {
    const limit = document.getElementById("dropoffLimit").value;
    const response = await fetch(`${API_BASE}/api/top-dropoff-zones?limit=${limit}`);
    const result = await response.json();

    const tbody = document.querySelector("#dropoffTable tbody");
    tbody.innerHTML = "";

    result.data.forEach(zone => {
        const row = `
            <tr>
                <td>${zone.rank}</td>
                <td>${zone.zone_name}</td>
                <td>${zone.trip_count}</td>
                <td>${zone.percentage}%</td>
            </tr>
        `;
        tbody.innerHTML += row;
    });
}

// RUSH HOUR CHARTS
let volumeChartInstance; //  global reference for toggle

function renderRushHourCharts() {
    const hours = Array.from({length: 24}, (_, i) => i); // 0-23 hours

    const peakHours = [8, 18]; // 8am and 6pm

    // BAR CHART: Volume
    const volumeCtx = document.getElementById('volumeChart').getContext('2d');
    const barColors = hours.map(h => (peakHours.includes(h) ? 'rgba(255, 99, 132, 0.7)' : 'rgba(54, 162, 235, 0.7)'));
    const borderColors = hours.map(h => (peakHours.includes(h) ? 'rgba(255, 99, 132, 1)' : 'rgba(54, 162, 235, 1)'));
    new Chart(volumeCtx, {
        type: 'bar',
        data: {
            labels: hours,
            datasets: [{
                label: 'Trip Volume',
                data: tripsByHour,
                backgroundColor: barColors,
                borderColor: borderColors,
                borderWidth: 1
            }]
        },
        options: {
            plugins: {
                title: {
                    display: true,
                    text: 'Trip Volume by Hour',
                    font: { size: 16 }
                },
                legend: { display: false }
            },
            scales: {
                x: { title: { display: true, text: 'Hour of Day' } },
                y: { title: { display: true, text: 'Trips' }, beginAtZero: true }
            }
        }
    });

    // Efficiency Chart (Avg Speed & Fare per Mile)
    const efficiencyCtx = document.getElementById('efficiencyChart').getContext('2d');
    new Chart(efficiencyCtx, {
        type: 'line',
        data: {
            labels: hours,
            datasets: [
                {
                    label: 'Avg Speed (mph)',
                    data: avgSpeedByHour,
                    borderColor: 'rgba(75, 192, 192, 1)',
                    backgroundColor: 'rgba(75, 192, 192, 0.2)',
                    yAxisID: 'ySpeed',
                    tension: 0.3
                },
                {
                    label: 'Fare per Mile ($)',
                    data: farePerMileByHour,
                    borderColor: 'rgba(255, 206, 86, 1)',
                    backgroundColor: 'rgba(255, 206, 86, 0.2)',
                    yAxisID: 'yFare',
                    tension: 0.3
                }
            ]
        },
        options: {
            plugins: {
                title: {
                    display: true,
                    text: 'Efficiency by Hour',
                    font: { size: 16 }
                }
            },
            scales: {
                x: { title: { display: true, text: 'Hour of Day' } },
                ySpeed: {
                    type: 'linear',
                    position: 'left',
                    title: { display: true, text: 'Avg Speed (mph)' },
                    beginAtZero: true
                },
                yFare: {
                    type: 'linear',
                    position: 'right',
                    title: { display: true, text: 'Fare per Mile ($)' },
                    beginAtZero: true,
                    grid: { drawOnChartArea: false } // prevents overlap
                }
            }
        }
    });

    // PEAK TOGGLE
    const peakToggle = document.getElementById('peakToggle');
    peakToggle.addEventListener('change', () => {
        // Updating bar colors based on toggle
        volumeChartInstance.data.datasets[0].backgroundColor = hours.map(h => 
            peakToggle.checked ? (peakHours.includes(h) ? 'rgba(255, 99, 132, 0.7)' : 'rgba(54, 162, 235, 0.7)') 
                                : 'rgba(54, 162, 235, 0.7)'
        );
        volumeChartInstance.data.datasets[0].borderColor = hours.map(h => 
            peakToggle.checked ? (peakHours.includes(h) ? 'rgba(255, 99, 132, 1)' : 'rgba(54, 162, 235, 1)') 
                                : 'rgba(54, 162, 235, 1)'
        );
        volumeChartInstance.update();
    });
}



// ALGORITHM INFO 
async function loadAlgorithmInfo() {
    const response = await fetch(`${API_BASE}/api/algorithm-info`);
    const result = await response.json();

    const container = document.getElementById("algorithmContainer");
    container.innerHTML = "";

    result.algorithms.forEach(algo => {
        const block = document.createElement("div");
        block.className = "stat-card";
        block.innerHTML = `
            <h3>${algo.name}</h3>
            <p><strong>Purpose:</strong> ${algo.purpose}</p>
            <p><strong>Time Complexity:</strong> ${algo.time_complexity}</p>
            <p><strong>Space Complexity:</strong> ${algo.space_complexity}</p>
        `;
        container.appendChild(block);
    });
}


// Auto-load statistics on page load
document.addEventListener("DOMContentLoaded", () => {
    loadStatistics();
    renderRushHourCharts();
});
