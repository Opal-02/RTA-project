async function fetchData() {
  const response = await fetch('/data');
  return await response.json();
}

async function fetchAlerts() {
  const response = await fetch('/alerts');
  return await response.json();
}

function updateAlerts(alerts) {
    const tbody = document.querySelector('#alertsTable tbody');
    tbody.innerHTML = '';
    alerts.forEach(alert => {
        const price = parseFloat(alert.price).toFixed(2);
        const change = parseFloat(alert.change_percent).toFixed(2);

        const date = new Date(alert.timestamp);
        const timestamp = `${date.getFullYear()}-${(date.getMonth()+1).toString().padStart(2, '0')}-${date.getDate().toString().padStart(2, '0')} ${(date.getHours()+2).toString().padStart(2, '0')}:${date.getMinutes().toString().padStart(2, '0')}:${date.getSeconds().toString().padStart(2, '0')}.${Math.floor(date.getMilliseconds() / 10).toString().padStart(2, '0')}`;

        const row = `<tr>
            <td>${alert.commodity}</td>
            <td>${price}</td>
            <td>${change}</td>
            <td>${alert.action}</td>
            <td>${timestamp}</td>
        </tr>`;
        tbody.innerHTML += row;
    });
}


function plotCommodity(prices, commodity, timeRangeMinutes) {
  let data = prices.filter(p => p.commodity === commodity);
  if (timeRangeMinutes !== 'all') {
    const cutoff = Date.now() - timeRangeMinutes * 60 * 1000;
    data = data.filter(d => new Date(d.timestamp).getTime() >= cutoff);
  }


  const x = data.map(d => d.timestamp);
  const y = data.map(d => d.price);

  const avg = y.reduce((a, b) => a + b, 0) / y.length;
  const avgLine = Array(y.length).fill(avg);

  const tracePrice = { x, y, mode: 'lines+markers', name: commodity };
  const traceAvg = { x, y: avgLine, mode: 'lines', name: 'Średnia', line: { dash: 'dot' } };

  const layout = {
    title: `Ceny: ${commodity} (${timeRangeMinutes === 'all' ? 'Całość' : 'Ostatnie ' + timeRangeMinutes + ' min'})`,
    xaxis: { title: 'Czas' },
    yaxis: { title: 'Cena' },
    margin: { t: 40 }
  };

  Plotly.newPlot('plot', [tracePrice, traceAvg], layout);
}

async function refresh() {
  const prices = await fetchData();
  const alerts = await fetchAlerts();
  updateAlerts(alerts);

  const commodities = [...new Set(prices.map(p => p.commodity))];
  const selector = document.getElementById('commoditySelector');
  const rangeSelector = document.getElementById('timeRange');

  // Wypełnij dropdown tylko raz
  if (selector.innerHTML === '') {
    commodities.forEach(commodity => {
      const option = document.createElement('option');
      option.value = commodity;
      option.text = commodity;
      selector.appendChild(option);
    });

    selector.addEventListener('change', () => {
      const selected = selector.value;
      const range = rangeSelector.value;
      plotCommodity(prices, selected, range);
    });

    rangeSelector.addEventListener('change', () => {
      const selected = selector.value;
      const range = rangeSelector.value;
      plotCommodity(prices, selected, range);
    });
  }

  const selected = selector.value || commodities[0];
  const range = rangeSelector.value || 'all';
  plotCommodity(prices, selected, range);
}

setInterval(refresh, 5000);
refresh();