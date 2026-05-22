# Real-Time Sensor Data Processing System

[![Python Version](https://img.shields.io/badge/python-3.9+-blue.svg)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.68+-green.svg)](https://fastapi.tiangolo.com)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

## Overview

A real-time sensor data processing system implementing asynchronous producer-consumer pattern with REST API and Server-Sent Events (SSE) streaming. Built for EN5500 Computer Systems Assignment 1.

## Features

- **Real-time Sensor Data Generation** - Simulates sensor readings with configurable error/missing rates
- **Asynchronous Processing** - Producer-consumer pattern with bounded queue for high throughput
- **Statistical Analysis** - Calculates max, min, mean, sample standard deviation (Bessel's correction), missing/corrupted counts
- **Runtime Configuration** - Change block size dynamically without restart
- **Timeout Watchdog** - Automatically flushes incomplete blocks after inactivity (default 5s)
- **REST API** - Full control and monitoring endpoints
- **SSE Streaming** - Real-time block updates via Server-Sent Events
- **Formatted Console Output** - Clean table display of all statistics per block

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run the application
python app.py
The server will start at http://localhost:8080

API Endpoints
Method	Endpoint	Description
GET	/	API documentation
GET	/status	System status and metrics
GET	/stats/latest	Latest block statistics
GET	/stats/all	All completed blocks
GET	/stream	SSE real-time stream
POST	/config/block-size	Change block size
POST	/control/start	Start processing
POST	/control/stop	Stop processing
POST	/control/reset	Reset system
Usage Examples
Change Block Size
bash
curl -X POST http://localhost:8080/config/block-size \
  -H "Content-Type: application/json" \
  -d '{"new_size": 50, "force_process": false}'
Get System Status
bash
curl http://localhost:8080/status
Stream Real-time Updates
bash
curl http://localhost:8080/stream
Console Output
When a block is processed, you'll see:

text
======================================================================
BLOCK #42
======================================================================
  Valid Samples:     95/100
  Max Value:         98
  Min Value:         12
  Mean:              54.32
  Std Deviation:     23.45
  Missing Samples:   3
  Corrupted Samples: 2
  Processing Time:   2.34 ms
======================================================================
Project Structure
text
sensor-data-processor/
├── app.py              # Main application
├── requirements.txt    # Dependencies
├── README.md          # Documentation
└── LICENSE            # MIT License

License
MIT

Author Luxman Madapatha
EN5500 - Computer Systems Assignment 1
