<script setup>
import HelloWorld from './components/HelloWorld.vue'
</script>

<template>
  Reports:
  <button @click="fetchReport">Get Report</button>


  <div v-if="reports.client_report">
    <h3>Client Report</h3>
    <table border="1">
      <tr>
        <th v-for="(value, key) in reports.client_report[0]" :key="key">{{ key }}</th>
      </tr>
      <tr v-for="item in reports.client_report" :key="item.client_id">
        <td v-for="(value, key) in item" :key="key">{{ value }}</td>
      </tr>
    </table>
  </div>

  <div v-if="reports.exchange_report">
    <h3>Exchange Report</h3>
    <table border="1">
      <tr>
        <th v-for="(value, key) in reports.exchange_report[0]" :key="key">{{ key }}</th>
      </tr>
      <tr v-for="item in reports.exchange_report" :key="item.order_id">
        <td v-for="(value, key) in item" :key="key">{{ value }}</td>
      </tr>
    </table>
  </div>

  <div v-if="reports.instrument_report">
    <h3>Instrument Report</h3>
    <table border="1">
      <tr>
        <th v-for="(value, key) in reports.instrument_report[0]" :key="key">{{ key }}</th>
      </tr>
      <tr v-for="item in reports.instrument_report" :key="item.instrument_id">
        <td v-for="(value, key) in item" :key="key">{{ value }}</td>
      </tr>
    </table>
  </div>
  <!-- <div>
    <a href="https://vitejs.dev" target="_blank">
      <img src="/vite.svg" class="logo" alt="Vite logo" />
    </a>
    <a href="https://vuejs.org/" target="_blank">
      <img src="./assets/vue.svg" class="logo vue" alt="Vue logo" />
    </a>
  </div>
  <HelloWorld msg="Vite + Vue" /> -->
</template>

<script>
import { ref } from 'vue';

const reports = ref({
  client_report: null,
  exchange_report: null,
  instrument_report: null
});

// Function to parse CSV data
function parseCSV(csv) {
  const lines = csv.split('\n');
  const result = [];
  const headers = lines[0].split(',');

  for (let i = 1; i < lines.length; i++) {
    if (!lines[i]) continue;
    const obj = {};
    const currentline = lines[i].split(',');

    for (let j = 0; j < headers.length; j++) {
      obj[headers[j]] = currentline[j];
    }
    result.push(obj);
  }
  return result;
}

const fetchReport = () => {
  // Simulated CSV data (replace these with actual fetch calls)

  fetch('http://localhost:5000/report')
    .then(response => response.json())
    .then(data => {
      console.log(data);
      Object.keys(data).forEach(reportType => {
        const reportKey = `${reportType}`;
        if (reports.value.hasOwnProperty(reportKey)) {
          reports.value[reportKey] = parseCSV(data[reportType]);
        }
      });
    })
    .catch(error => console.error('Error fetching reports:', error));


  // const clientCSV = "client_id,order_id,reason\n1,1,invalid policy\n";
  // const exchangeCSV = "order_id,reason\n1,invalid policy\n";
  // const instrumentCSV = "instrument_id,open_price,closed_price,total_traded_vol,day_high,day_low,vwap,timestamp\nSIA,30,43,400,43,29,142,1715582657\n";

  // reports.value.clientReport = parseCSV(clientCSV);
  // reports.value.exchangeReport = parseCSV(exchangeCSV);
  // reports.value.instrumentReport = parseCSV(instrumentCSV);
};

// fetchReport();
</script>

<style scoped>
.logo {
  height: 6em;
  padding: 1.5em;
  will-change: filter;
  transition: filter 300ms;
}

.logo:hover {
  filter: drop-shadow(0 0 2em #646cffaa);
}

.logo.vue:hover {
  filter: drop-shadow(0 0 2em #42b883aa);
}
</style>
