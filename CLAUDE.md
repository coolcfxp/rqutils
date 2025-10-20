# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Structure

This is a Java utility library (`rqutils`) organized as a multi-module Maven project with three main modules:

- **patterns**: Thread utilities and design patterns
  - `threadutils.executor`: Multi-threaded executors like `HashedMultiExecutor`
  - `threadutils.factory`: Get-or-create factories with caching and size limits
  - `threadutils.pool`: Object pooling implementations
  - `threadutils.buffer`: Ring buffer implementations
  - `threadutils.async`: Async request management utilities
  - `threadutils.id`: ID generation utilities

- **io-tools**: File I/O and data processing utilities
  - File tailers for monitoring file changes (CSV, DBF, text, binary)
  - CSV readers/writers with field-based data access
  - DBF (dBase) file format support with codec functionality

- **network**: Vert.x-based networking components
  - WebSocket server and client implementations using Vert.x
  - Service processor pattern for handling different message types
  - Binary message senders with session management
  - Connection credential management

## Technology Stack

- **Java 21** (JDK 21 required)
- **Maven 3.x** for build management
- **Lombok** for code generation (fluent accessors enabled)
- **Vert.x 4.5.14** for async networking (network module only)
- **JUnit 5** and **Mockito** for testing
- **Apache Commons IO** for I/O utilities
- **FastUtil** for high-performance collections
- **Caffeine** for caching

## Build Commands

```bash
# Build all modules
mvn clean compile

# Run tests for all modules
mvn test

# Build and package all modules
mvn clean package

# Test a specific module
mvn test -pl patterns
mvn test -pl io-tools
mvn test -pl network

# Run a single test class
mvn test -Dtest=HashedMultiExecutorTest -pl patterns
```

## Code Conventions

- Use **Lombok** extensively with fluent accessors (`@Accessors(fluent = true)`)
- Follow **Vert.x coding patterns** in the network module
- Always guard debug log statements with level checks
- Use builder patterns for complex object construction
- Package structure follows `com.ricequant.rqutils.[module]` naming

## Architecture Notes

- **Thread Safety**: Most components are designed for concurrent access with proper synchronization
- **Builder Pattern**: Complex classes like tailers and writers use builder patterns for configuration
- **Service Processor Pattern**: Network module uses a service processor abstraction for handling different message types
- **Session Management**: WebSocket connections maintain logical sessions with reconnection support
- **File Monitoring**: IO tools provide real-time file monitoring capabilities with customizable listeners

## Key Dependencies

- The network module depends on Vert.x for async I/O and WebSocket handling
- All modules include Lombok for reducing boilerplate code
- Git commit information is automatically embedded in JAR manifests during build
- Target directories contain compiled classes and should be ignored in version control