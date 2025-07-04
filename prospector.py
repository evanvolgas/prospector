#!/usr/bin/env python3
"""
Prospector Risk Calculator - Master Control Script

This script provides a unified interface to manage all Prospector components.
"""

import subprocess
import sys
import time
import argparse
import signal
import os
from typing import List, Dict, Optional
import psutil
import docker
import redis


class ProspectorController:
    def __init__(self):
        self.processes: Dict[str, subprocess.Popen] = {}
        self.docker_client = None
        self.redis_client = None
        
        # Try to connect to Docker
        try:
            self.docker_client = docker.from_env()
        except:
            print("⚠️  Docker not available. Please ensure Docker is running.")
        
        # Component commands
        self.components = {
            'risk-calculator': ['uv', 'run', 'risk-calculator'],
            'data-generator': ['uv', 'run', 'data-generator'],
            'api': ['uv', 'run', 'risk-api'],
            'monitor': ['uv', 'run', 'benchmark-monitor'],
            'dashboard': ['uv', 'run', 'performance-dashboard']
        }
        
    def check_docker_services(self) -> Dict[str, bool]:
        """Check if required Docker services are running."""
        required_services = {
            'kafka': False,
            'redis': False,
            'kafka-ui': False
        }
        
        if not self.docker_client:
            return required_services
        
        try:
            containers = self.docker_client.containers.list()
            for container in containers:
                name = container.name
                if name in required_services:
                    required_services[name] = container.status == 'running'
        except Exception as e:
            print(f"Error checking Docker services: {e}")
        
        return required_services
    
    def start_infrastructure(self, force: bool = False) -> bool:
        """Start infrastructure services if not already running."""
        services = self.check_docker_services()
        
        kafka_running = services.get('kafka', False)
        redis_running = services.get('redis', False)
        
        if kafka_running and redis_running and not force:
            print("✅ Infrastructure already running (Kafka and Redis)")
            return True
        
        print("🚀 Starting infrastructure services...")
        
        try:
            # Use docker-compose to start services
            cmd = ['docker-compose', 'up', '-d']
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode != 0:
                print(f"❌ Failed to start infrastructure: {result.stderr}")
                return False
            
            # Wait for services to be healthy
            print("⏳ Waiting for services to be healthy...")
            max_attempts = 30
            for i in range(max_attempts):
                services = self.check_docker_services()
                if services.get('kafka') and services.get('redis'):
                    print("✅ Infrastructure is ready!")
                    return True
                time.sleep(2)
                print(".", end="", flush=True)
            
            print("\n❌ Infrastructure failed to become healthy")
            return False
            
        except Exception as e:
            print(f"❌ Error starting infrastructure: {e}")
            return False
    
    def stop_infrastructure(self):
        """Stop infrastructure services."""
        print("🛑 Stopping infrastructure services...")
        try:
            subprocess.run(['docker-compose', 'down'], capture_output=True)
            print("✅ Infrastructure stopped")
        except Exception as e:
            print(f"❌ Error stopping infrastructure: {e}")
    
    def start_component(self, name: str, args: List[str] = None) -> bool:
        """Start a specific component."""
        if name in self.processes and self.processes[name].poll() is None:
            print(f"⚠️  {name} is already running")
            return True
        
        if name not in self.components:
            print(f"❌ Unknown component: {name}")
            return False
        
        cmd = self.components[name].copy()
        if args:
            cmd.extend(args)
        
        print(f"🚀 Starting {name}...")
        try:
            if name == 'dashboard':
                # Dashboard needs to run in foreground for matplotlib
                self.processes[name] = subprocess.Popen(cmd)
            else:
                self.processes[name] = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True
                )
            print(f"✅ {name} started")
            return True
        except Exception as e:
            print(f"❌ Failed to start {name}: {e}")
            return False
    
    def stop_component(self, name: str):
        """Stop a specific component."""
        if name not in self.processes:
            return
        
        proc = self.processes[name]
        if proc.poll() is None:
            print(f"🛑 Stopping {name}...")
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
            print(f"✅ {name} stopped")
        
        del self.processes[name]
    
    def stop_all_components(self):
        """Stop all running components."""
        for name in list(self.processes.keys()):
            self.stop_component(name)
    
    def status(self):
        """Show status of all components."""
        print("\n" + "="*60)
        print("PROSPECTOR STATUS")
        print("="*60)
        
        # Infrastructure status
        print("\n📦 Infrastructure:")
        services = self.check_docker_services()
        for service, running in services.items():
            status = "✅ Running" if running else "❌ Stopped"
            print(f"  {service}: {status}")
        
        # Component status
        print("\n🔧 Components:")
        for name in self.components:
            if name in self.processes and self.processes[name].poll() is None:
                print(f"  {name}: ✅ Running (PID: {self.processes[name].pid})")
            else:
                print(f"  {name}: ❌ Stopped")
        
        # Redis stats
        try:
            r = redis.Redis(host='localhost', port=6379, decode_responses=True)
            info = r.info()
            risk_keys = len(r.keys("risk:*"))
            print(f"\n📊 Redis Stats:")
            print(f"  Risk calculations cached: {risk_keys}")
            print(f"  Memory used: {info.get('used_memory_human', 'N/A')}")
        except:
            print("\n📊 Redis Stats: Not available")
        
        print("\n" + "="*60)
    
    def run_demo(self):
        """Run a demo with all components."""
        print("\n🎬 Starting Prospector Demo Mode...")
        
        # Start infrastructure
        if not self.start_infrastructure():
            print("❌ Failed to start infrastructure")
            return
        
        time.sleep(3)
        
        # Start risk calculator
        self.start_component('risk-calculator')
        time.sleep(2)
        
        # Start API
        self.start_component('api')
        time.sleep(2)
        
        # Start monitoring
        self.start_component('monitor')
        
        print("\n✅ Demo mode started!")
        print("\n📝 Next steps:")
        print("  1. Generate test data: prospector generate --portfolios 100")
        print("  2. View API: http://localhost:6066")
        print("  3. View Kafka UI: http://localhost:8080")
        print("  4. Start dashboard: prospector start dashboard")
        print("\n Press Ctrl+C to stop all services")
    
    def run_benchmark(self, portfolios: int = 1000, updates: int = 5):
        """Run a complete benchmark."""
        print(f"\n🏃 Running benchmark with {portfolios} portfolios...")
        
        # Ensure infrastructure is running
        if not self.start_infrastructure():
            return
        
        # Clean Redis
        try:
            r = redis.Redis(host='localhost', port=6379, decode_responses=True)
            for key in r.keys("risk:*"):
                r.delete(key)
            for key in r.keys("stats:*"):
                r.delete(key)
            r.delete("global:metrics")
            print("✅ Cleaned previous data")
        except:
            pass
        
        # Run the benchmark
        cmd = ['uv', 'run', 'run-benchmark', '--portfolios', str(portfolios), '--updates', str(updates)]
        subprocess.run(cmd)
    
    def generate_data(self, mode: str = 'continuous', portfolios: int = 100, updates: int = 5):
        """Generate test data."""
        cmd = ['uv', 'run', 'data-generator', '--mode', mode]
        
        if mode == 'batch':
            cmd.extend(['--num-portfolios', str(portfolios), '--updates-per-portfolio', str(updates)])
        
        print(f"📊 Generating data ({mode} mode)...")
        subprocess.run(cmd)


def main():
    parser = argparse.ArgumentParser(
        description='Prospector Risk Calculator - Master Control',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  prospector start all          # Start all services
  prospector start infra        # Start only infrastructure
  prospector start calc         # Start risk calculator
  prospector stop all           # Stop everything
  prospector status             # Show component status
  prospector demo               # Run demo mode
  prospector benchmark          # Run performance benchmark
  prospector generate           # Generate test data
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # Start command
    start_parser = subparsers.add_parser('start', help='Start components')
    start_parser.add_argument('component', choices=['all', 'infra', 'calc', 'api', 'monitor', 'dashboard'],
                            help='Component to start')
    start_parser.add_argument('--workers', type=int, help='Number of workers for risk calculator')
    
    # Stop command
    stop_parser = subparsers.add_parser('stop', help='Stop components')
    stop_parser.add_argument('component', choices=['all', 'infra', 'calc', 'api', 'monitor', 'dashboard'],
                           help='Component to stop')
    
    # Status command
    subparsers.add_parser('status', help='Show status of all components')
    
    # Demo command
    subparsers.add_parser('demo', help='Run demo mode with all components')
    
    # Benchmark command
    bench_parser = subparsers.add_parser('benchmark', help='Run performance benchmark')
    bench_parser.add_argument('--portfolios', type=int, default=1000, help='Number of portfolios')
    bench_parser.add_argument('--updates', type=int, default=5, help='Updates per portfolio')
    
    # Generate command
    gen_parser = subparsers.add_parser('generate', help='Generate test data')
    gen_parser.add_argument('--mode', choices=['continuous', 'batch'], default='batch')
    gen_parser.add_argument('--portfolios', type=int, default=100, help='Number of portfolios')
    gen_parser.add_argument('--updates', type=int, default=5, help='Updates per portfolio')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    controller = ProspectorController()
    
    # Set up signal handler
    def signal_handler(sig, frame):
        print('\n\n🛑 Shutting down...')
        controller.stop_all_components()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        if args.command == 'start':
            if args.component == 'all':
                controller.start_infrastructure()
                controller.start_component('risk-calculator', 
                                         ['--workers', str(args.workers)] if args.workers else None)
                controller.start_component('api')
                controller.start_component('monitor')
                print("\n✅ All components started!")
                print("   View API: http://localhost:6066")
                print("   View Kafka UI: http://localhost:8080")
                print("   Press Ctrl+C to stop")
                # Keep running
                signal.pause()
            elif args.component == 'infra':
                controller.start_infrastructure()
            elif args.component == 'calc':
                controller.start_component('risk-calculator',
                                         ['--workers', str(args.workers)] if args.workers else None)
            else:
                controller.start_component(args.component)
        
        elif args.command == 'stop':
            if args.component == 'all':
                controller.stop_all_components()
                controller.stop_infrastructure()
            elif args.component == 'infra':
                controller.stop_infrastructure()
            else:
                controller.stop_component(args.component)
        
        elif args.command == 'status':
            controller.status()
        
        elif args.command == 'demo':
            controller.run_demo()
            # Keep running until Ctrl+C
            signal.pause()
        
        elif args.command == 'benchmark':
            controller.run_benchmark(args.portfolios, args.updates)
        
        elif args.command == 'generate':
            controller.generate_data(args.mode, args.portfolios, args.updates)
    
    except KeyboardInterrupt:
        print("\n\nShutting down...")
        controller.stop_all_components()


if __name__ == "__main__":
    main()