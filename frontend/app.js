//Intializing frontend
const API_BASE = "http://127.0.0.1:5000"; 

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



// Auto-load statistics on page load
document.addEventListener("DOMContentLoaded", () => {
    loadStatistics();
});
