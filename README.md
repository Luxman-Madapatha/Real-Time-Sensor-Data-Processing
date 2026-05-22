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

## Tech Stack

- Python 3.9+
- FastAPI - Web framework
- Uvicorn - ASGI server
- Pydantic - Data validation
- Asyncio - Concurrent processing

## Installation

```bash
# Clone repository
git clone https://github.com/Luxman-Madapatha/Real-Time-Sensor-Data-Processing.git
cd sensor-data-processor

# Install dependencies
pip install -r requirements.txt

# Run application
python app.py
