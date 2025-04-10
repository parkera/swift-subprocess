//===----------------------------------------------------------------------===//
//
// This source file is part of the Swift.org open source project
//
// Copyright (c) 2025 Apple Inc. and the Swift project authors
// Licensed under Apache License v2.0 with Runtime Library Exception
//
// See https://swift.org/LICENSE.txt for license information
//
//===----------------------------------------------------------------------===//

#if canImport(System)
@preconcurrency import System
#else
@preconcurrency import SystemPackage
#endif

#if canImport(Darwin)
import Darwin
#elseif canImport(Android)
import Android
#elseif canImport(Glibc)
import Glibc
#elseif canImport(Musl)
import Musl
#elseif canImport(WinSDK)
import WinSDK
#endif

internal import Dispatch

/// A collection of configurations parameters to use when
/// spawning a subprocess.
public struct Configuration: Sendable {
    /// The executable to run.
    public var executable: Executable
    /// The arguments to pass to the executable.
    public var arguments: Arguments
    /// The environment to use when running the executable.
    public var environment: Environment
    /// The working directory to use when running the executable.
    public var workingDirectory: FilePath
    /// The platform specifc options to use when
    /// running the subprocess.
    public var platformOptions: PlatformOptions

    public init(
        executable: Executable,
        arguments: Arguments = [],
        environment: Environment = .inherit,
        workingDirectory: FilePath? = nil,
        platformOptions: PlatformOptions = PlatformOptions()
    ) {
        self.executable = executable
        self.arguments = arguments
        self.environment = environment
        self.workingDirectory = workingDirectory ?? .currentWorkingDirectory
        self.platformOptions = platformOptions
    }

    #if SubprocessSpan
    @available(SubprocessSpan, *)
    #endif
    internal func run<
        Result,
        Output: OutputProtocol,
        Error: OutputProtocol
    >(
        output: Output,
        error: Error,
        isolation: isolated (any Actor)? = #isolation,
        _ body: (
            borrowing Execution<Output, Error>,
            borrowing StandardInputWriter
        ) async throws -> Result
    ) async throws -> ExecutionResult<Result> {
        let input = CustomWriteInput()

        let inputPipe = try PipeCreator(input)
        let outputPipe = try PipeCreator(output)
        let errorPipe = try PipeCreator(error)
        let inputRead = inputPipe.read
        let inputWrite = inputPipe.write
        let outputRead = outputPipe.read
        let outputWrite = outputPipe.write
        let errorRead = errorPipe.read
        let errorWrite = errorPipe.write
        
        let execution = try self.spawn(
            inputRead: inputRead,
            inputWrite: inputWrite,
            output: output,
            outputRead: outputRead,
            outputWrite: outputWrite,
            error: error,
            errorRead: errorRead,
            errorWrite: errorWrite
        )
                
        return try await withAsyncTaskCleanupHandler(execution: execution, input: inputWrite) { execution, fd in
            let pid = execution.processIdentifier
            async let waitingStatus = try await monitorProcessTermination(
                forProcessWithIdentifier: pid
            )
            // Body runs in the same isolation
            let standardInputWriter = StandardInputWriter(fileDescriptor: fd!)
            let result = try await body(
                execution,
                standardInputWriter
            )
            try await standardInputWriter.finish()
            return ExecutionResult(
                terminationStatus: try await waitingStatus,
                value: result
            )
        }
    }

    #if SubprocessSpan
    @available(SubprocessSpan, *)
    internal func run<
        InputElement: BitwiseCopyable,
        Output: OutputProtocol,
        Error: OutputProtocol
    >(
        input: borrowing Span<InputElement>,
        output: Output,
        error: Error,
        isolation: isolated (any Actor)? = #isolation
    ) async throws -> CollectedResult<Output, Error> {
        let writerInput = CustomWriteInput()

        let inputPipe = try PipeCreator(writerInput)
        let outputPipe = try PipeCreator(output)
        let errorPipe = try PipeCreator(error)
        let inputRead = inputPipe.read
        let inputWrite = inputPipe.write
        let outputRead = outputPipe.read
        let outputWrite = outputPipe.write
        let errorRead = errorPipe.read
        let errorWrite = errorPipe.write
        
        let execution = try self.spawn(
            inputRead: inputRead,
            inputWrite: inputWrite,
            output: output,
            outputRead: outputRead,
            outputWrite: outputWrite,
            error: error,
            errorRead: errorRead,
            errorWrite: errorWrite
        )
        
        return try await withAsyncTaskCleanupHandler(execution: execution, input: inputWrite) { execution, fd in
            // Spawn parallel tasks to monitor exit status
            // and capture outputs. Input writing must happen
            // in this scope for Span
            let pid = execution.processIdentifier
            async let terminationStatus = try monitorProcessTermination(
                forProcessWithIdentifier: pid
            )
            // Write input in the same scope
            guard let writeFd = fd else {
                fatalError("Trying to write to an input that has been closed")
            }
            try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, any Swift.Error>) in
                input.withUnsafeBytes { ptr in
                    #if os(Windows)
                    let bytes = ptr
                    #else
                    let bytes = DispatchData(
                        bytesNoCopy: ptr,
                        deallocator: .custom(
                            nil,
                            {
                                // noop
                            }
                        )
                    )
                    #endif

                    writeFd.platformDescriptor.write(bytes) { _, error in
                        if let error = error {
                            continuation.resume(throwing: error)
                        } else {
                            continuation.resume()
                        }
                    }
                }

            }
            try writeFd.safelyClose()
            let (
                standardOutput,
                standardError
            ) = try await execution.captureIOs()
            return CollectedResult<Output, Error>(
                processIdentifier: pid,
                terminationStatus: try await terminationStatus,
                standardOutput: standardOutput,
                standardError: standardError
            )
        }
    }
    #endif  // SubprocessSpan
    
    #if SubprocessSpan
    @available(SubprocessSpan, *)
    #endif
    internal func run<
        Result,
        Input: InputProtocol,
        Output: OutputProtocol,
        Error: OutputProtocol
    >(
        input: Input,
        output: Output,
        error: Error,
        isolation: isolated (any Actor)? = #isolation,
        _ body: ((consuming Execution<Output, Error>) async throws -> Result)
    ) async throws -> ExecutionResult<Result> {

        let inputPipe = try PipeCreator(input)
        let outputPipe = try PipeCreator(output)
        let errorPipe = try PipeCreator(error)
        let inputRead = inputPipe.read
        let inputWrite = inputPipe.write
        let outputRead = outputPipe.read
        let outputWrite = outputPipe.write
        let errorRead = errorPipe.read
        let errorWrite = errorPipe.write
        
        let execution = try self.spawn(
            inputRead: inputRead,
            inputWrite: inputWrite,
            output: output,
            outputRead: outputRead,
            outputWrite: outputWrite,
            error: error,
            errorRead: errorRead,
            errorWrite: errorWrite
        )
        
        return try await withAsyncTaskCleanupHandler(execution: execution, input: inputWrite) { execution, fd in
            // Use a series of boxes to get the file descriptor into the one place we use it below. This dynamically enforces a 'once' pattern on the closure.
            let pid = execution.processIdentifier
            var fdBox: TrackedFileDescriptor?? = consume fd
            var executionBox : Execution? = consume execution
            return try await withThrowingTaskGroup(
                of: TerminationStatus?.self,
                returning: ExecutionResult.self
            ) { group in
                let fdAgain = fdBox?.take()!
                var fdBox2 : TrackedFileDescriptor?? = consume fdAgain
                group.addTask {
                    let fd = fdBox2.take()!
                    if let fd {
                        let writer = StandardInputWriter(fileDescriptor: fd)
                        try await input.write(with: writer)
                        try await writer.finish()
                    }
                    return nil
                }
                group.addTask {
                    return try await monitorProcessTermination(
                        forProcessWithIdentifier: pid
                    )
                }

                // Body runs in the same isolation
                let execution = executionBox.take()!
                let result = try await body(execution)
                var status: TerminationStatus? = nil
                while let monitorResult = try await group.next() {
                    if let monitorResult = monitorResult {
                        status = monitorResult
                    }
                }
                return ExecutionResult(terminationStatus: status!, value: result)
            }
        }
    }
}

extension Configuration: CustomStringConvertible, CustomDebugStringConvertible {
    public var description: String {
        return """
            Configuration(
                executable: \(self.executable.description),
                arguments: \(self.arguments.description),
                environment: \(self.environment.description),
                workingDirectory: \(self.workingDirectory),
                platformOptions: \(self.platformOptions.description(withIndent: 1))
            )
            """
    }

    public var debugDescription: String {
        return """
            Configuration(
                executable: \(self.executable.debugDescription),
                arguments: \(self.arguments.debugDescription),
                environment: \(self.environment.debugDescription),
                workingDirectory: \(self.workingDirectory),
                platformOptions: \(self.platformOptions.description(withIndent: 1))
            )
            """
    }

}

// MARK: - Cleanup
extension Configuration {
//    /// Close each input individually, and throw the first error if there's multiple errors thrown
//    @Sendable
//    #if SubprocessSpan
//    @available(SubprocessSpan, *)
//    #endif
//    private func cleanup<
//        Output: OutputProtocol,
//        Error: OutputProtocol
//    >(
//        execution: consuming Execution<Output, Error>,
//        input: consuming TrackedFileDescriptor?,
//        attemptToTerminateSubProcess: Bool
//    ) async throws {
//        func captureError(_ work: () throws -> Void) -> Swift.Error? {
//            do {
//                try work()
//                return nil
//            } catch {
//                // Ignore badFileDescriptor for double close
//                return error
//            }
//        }
//
//        // Attempt to teardown the subprocess
//        if attemptToTerminateSubProcess {
//            await execution.teardown(
//                using: self.platformOptions.teardownSequence
//            )
//        }
//
//        var inputError: Swift.Error?
//        var executionError: Swift.Error?
//        
//        // Make sure to always call close, then re-throw the first error that we find
//        inputError = captureError {
//            try input?.safelyClose()
//        }
//        
//        executionError = captureError {
//            try execution.cleanup()
//        }
//        
//        if let inputError = inputError {
//            throw inputError
//        }
//        
//        if let executionError {
//            throw executionError
//        }
//    }
//
//    /// Close each input individually, and throw the first error if there's multiple errors thrown
//    @Sendable
//    internal func cleanupPreSpawn(
//        inputRead: consuming TrackedFileDescriptor?,
//        inputWrite: consuming TrackedFileDescriptor?,
//        outputRead: consuming TrackedFileDescriptor?,
//        outputWrite: consuming TrackedFileDescriptor?,
//        errorRead: consuming TrackedFileDescriptor?,
//        errorWrite: consuming TrackedFileDescriptor?
//    ) throws {
//        var inputError: Swift.Error?
//        var outputError: Swift.Error?
//        var errorError: Swift.Error?
//
//        do {
//            try inputRead?.safelyClose()
//            try inputWrite?.safelyClose()
//        } catch {
//            inputError = error
//        }
//
//        do {
//            try outputRead?.safelyClose()
//            try outputWrite?.safelyClose()
//        } catch {
//            outputError = error
//        }
//
//        do {
//            try errorRead?.safelyClose()
//            try errorWrite?.safelyClose()
//        } catch {
//            errorError = error
//        }
//
//        if let inputError = inputError {
//            throw inputError
//        }
//        if let outputError = outputError {
//            throw outputError
//        }
//        if let errorError = errorError {
//            throw errorError
//        }
//    }
    
    // MARK: -
        
    #if SubprocessSpan
    @available(SubprocessSpan, *)
    #endif
    private func withAsyncTaskCleanupHandler<Result, Output: OutputProtocol, Error: OutputProtocol>(
        execution: consuming Execution<Output, Error>,
        input: consuming TrackedFileDescriptor?,
        _ body: (consuming Execution<Output, Error>, consuming TrackedFileDescriptor?) async throws -> Result,
        isolation: isolated (any Actor)? = #isolation
    ) async rethrows -> Result {
        let pid = execution.processIdentifier
        var executionBox : Execution<Output, Error>? = consume execution
        var inputBox : TrackedFileDescriptor?? = consume input
        return try await withThrowingTaskGroup(
            of: Void.self,
            returning: Result.self
        ) { group in
            group.addTask {
                // Keep this task sleep indefinitely until the parent task is cancelled.
                // `Task.sleep` throws `CancellationError` when the task is canceled
                // before the time ends. We then run the cancel handler.
                do { while true { try await Task.sleep(nanoseconds: 1_000_000_000) } } catch {}
                // Run task cancel handler
                
                // Attempt to terminate the child process
                // Since the task has already been cancelled,
                // this is the best we can do
                await Execution<Output, Error>.teardown(
                    processIdentifier: pid,
                    using: self.platformOptions.teardownSequence
                )
            }

            do {
                // Unwrap from the box, which enforces that the closure is run only once
                let execution = executionBox.take()!
                let input = inputBox.take()!
                let result = try await body(execution, input)
                group.cancelAll()
                return result
            } catch {
                await Execution<Output, Error>.teardown(
                    processIdentifier: pid,
                    using: self.platformOptions.teardownSequence
                )
                throw error
            }
        }
    }

}

// MARK: - Executable

/// `Executable` defines how the executable should
/// be looked up for execution.
public struct Executable: Sendable, Hashable {
    internal enum Storage: Sendable, Hashable {
        case executable(String)
        case path(FilePath)
    }

    internal let storage: Storage

    private init(_config: Storage) {
        self.storage = _config
    }

    /// Locate the executable by its name.
    /// `Subprocess` will use `PATH` value to
    /// determine the full path to the executable.
    public static func name(_ executableName: String) -> Self {
        return .init(_config: .executable(executableName))
    }
    /// Locate the executable by its full path.
    /// `Subprocess` will use this  path directly.
    public static func path(_ filePath: FilePath) -> Self {
        return .init(_config: .path(filePath))
    }
    /// Returns the full executable path given the environment value.
    public func resolveExecutablePath(in environment: Environment) throws -> FilePath {
        let path = try self.resolveExecutablePath(withPathValue: environment.pathValue())
        return FilePath(path)
    }
}

extension Executable: CustomStringConvertible, CustomDebugStringConvertible {
    public var description: String {
        switch storage {
        case .executable(let executableName):
            return executableName
        case .path(let filePath):
            return filePath.string
        }
    }

    public var debugDescription: String {
        switch storage {
        case .executable(let string):
            return "executable(\(string))"
        case .path(let filePath):
            return "path(\(filePath.string))"
        }
    }
}

extension Executable {
    internal func possibleExecutablePaths(
        withPathValue pathValue: String?
    ) -> Set<String> {
        switch self.storage {
        case .executable(let executableName):
            #if os(Windows)
            // Windows CreateProcessW accepts executable name directly
            return Set([executableName])
            #else
            var results: Set<String> = []
            // executableName could be a full path
            results.insert(executableName)
            // Get $PATH from environment
            let searchPaths: Set<String>
            if let pathValue = pathValue {
                let localSearchPaths = pathValue.split(separator: ":").map { String($0) }
                searchPaths = Set(localSearchPaths).union(Self.defaultSearchPaths)
            } else {
                searchPaths = Self.defaultSearchPaths
            }
            for path in searchPaths {
                results.insert(
                    FilePath(path).appending(executableName).string
                )
            }
            return results
            #endif
        case .path(let executablePath):
            return Set([executablePath.string])
        }
    }
}

// MARK: - Arguments

/// A collection of arguments to pass to the subprocess.
public struct Arguments: Sendable, ExpressibleByArrayLiteral, Hashable {
    public typealias ArrayLiteralElement = String

    internal let storage: [StringOrRawBytes]
    internal let executablePathOverride: StringOrRawBytes?

    /// Create an Arguments object using the given literal values
    public init(arrayLiteral elements: String...) {
        self.storage = elements.map { .string($0) }
        self.executablePathOverride = nil
    }
    /// Create an Arguments object using the given array
    public init(_ array: [String]) {
        self.storage = array.map { .string($0) }
        self.executablePathOverride = nil
    }

    #if !os(Windows)  // Windows does NOT support arg0 override
    /// Create an `Argument` object using the given values, but
    /// override the first Argument value to `executablePathOverride`.
    /// If `executablePathOverride` is nil,
    /// `Arguments` will automatically use the executable path
    /// as the first argument.
    /// - Parameters:
    ///   - executablePathOverride: the value to override the first argument.
    ///   - remainingValues: the rest of the argument value
    public init(executablePathOverride: String?, remainingValues: [String]) {
        self.storage = remainingValues.map { .string($0) }
        if let executablePathOverride = executablePathOverride {
            self.executablePathOverride = .string(executablePathOverride)
        } else {
            self.executablePathOverride = nil
        }
    }

    /// Create an `Argument` object using the given values, but
    /// override the first Argument value to `executablePathOverride`.
    /// If `executablePathOverride` is nil,
    /// `Arguments` will automatically use the executable path
    /// as the first argument.
    /// - Parameters:
    ///   - executablePathOverride: the value to override the first argument.
    ///   - remainingValues: the rest of the argument value
    public init(executablePathOverride: [UInt8]?, remainingValues: [[UInt8]]) {
        self.storage = remainingValues.map { .rawBytes($0) }
        if let override = executablePathOverride {
            self.executablePathOverride = .rawBytes(override)
        } else {
            self.executablePathOverride = nil
        }
    }

    public init(_ array: [[UInt8]]) {
        self.storage = array.map { .rawBytes($0) }
        self.executablePathOverride = nil
    }
    #endif
}

extension Arguments: CustomStringConvertible, CustomDebugStringConvertible {
    public var description: String {
        var result: [String] = self.storage.map(\.description)

        if let override = self.executablePathOverride {
            result.insert("override\(override.description)", at: 0)
        }
        return result.description
    }

    public var debugDescription: String { return self.description }
}

// MARK: - Environment

/// A set of environment variables to use when executing the subprocess.
public struct Environment: Sendable, Hashable {
    internal enum Configuration: Sendable, Hashable {
        case inherit([String: String])
        case custom([String: String])
        #if !os(Windows)
        case rawBytes([[UInt8]])
        #endif
    }

    internal let config: Configuration

    init(config: Configuration) {
        self.config = config
    }
    /// Child process should inherit the same environment
    /// values from its parent process.
    public static var inherit: Self {
        return .init(config: .inherit([:]))
    }
    /// Override the provided `newValue` in the existing `Environment`
    public func updating(_ newValue: [String: String]) -> Self {
        return .init(config: .inherit(newValue))
    }
    /// Use custom environment variables
    public static func custom(_ newValue: [String: String]) -> Self {
        return .init(config: .custom(newValue))
    }

    #if !os(Windows)
    /// Use custom environment variables of raw bytes
    public static func custom(_ newValue: [[UInt8]]) -> Self {
        return .init(config: .rawBytes(newValue))
    }
    #endif
}

extension Environment: CustomStringConvertible, CustomDebugStringConvertible {
    public var description: String {
        switch self.config {
        case .custom(let customDictionary):
            return """
                Custom environment:
                \(customDictionary)
                """
        case .inherit(let updateValue):
            return """
                Inherting current environment with updates:
                \(updateValue)
                """
        #if !os(Windows)
        case .rawBytes(let rawBytes):
            return """
                Raw bytes:
                \(rawBytes)
                """
        #endif
        }
    }

    public var debugDescription: String {
        return self.description
    }

    internal static func currentEnvironmentValues() -> [String: String] {
        return self.withCopiedEnv { environments in
            var results: [String: String] = [:]
            for env in environments {
                let environmentString = String(cString: env)

                #if os(Windows)
                // Windows GetEnvironmentStringsW API can return
                // magic environment variables set by the cmd shell
                // that starts with `=`
                // We should exclude these values
                if environmentString.utf8.first == Character("=").utf8.first {
                    continue
                }
                #endif  // os(Windows)

                guard let delimiter = environmentString.firstIndex(of: "=") else {
                    continue
                }

                let key = String(environmentString[environmentString.startIndex..<delimiter])
                let value = String(
                    environmentString[environmentString.index(after: delimiter)..<environmentString.endIndex]
                )
                results[key] = value
            }
            return results
        }
    }
}

// MARK: - TerminationStatus

/// An exit status of a subprocess.
@frozen
public enum TerminationStatus: Sendable, Hashable, Codable {
    #if canImport(WinSDK)
    public typealias Code = DWORD
    #else
    public typealias Code = CInt
    #endif

    /// The subprocess was existed with the given code
    case exited(Code)
    /// The subprocess was signalled with given exception value
    case unhandledException(Code)
    /// Whether the current TerminationStatus is successful.
    public var isSuccess: Bool {
        switch self {
        case .exited(let exitCode):
            return exitCode == 0
        case .unhandledException(_):
            return false
        }
    }
}

extension TerminationStatus: CustomStringConvertible, CustomDebugStringConvertible {
    public var description: String {
        switch self {
        case .exited(let code):
            return "exited(\(code))"
        case .unhandledException(let code):
            return "unhandledException(\(code))"
        }
    }

    public var debugDescription: String {
        return self.description
    }
}

// MARK: - Internal

internal enum StringOrRawBytes: Sendable, Hashable {
    case string(String)
    case rawBytes([UInt8])

    // Return value needs to be deallocated manually by callee
    func createRawBytes() -> UnsafeMutablePointer<CChar> {
        switch self {
        case .string(let string):
            return strdup(string)
        case .rawBytes(let rawBytes):
            return strdup(rawBytes)
        }
    }

    var stringValue: String? {
        switch self {
        case .string(let string):
            return string
        case .rawBytes(let rawBytes):
            return String(decoding: rawBytes, as: UTF8.self)
        }
    }

    var description: String {
        switch self {
        case .string(let string):
            return string
        case .rawBytes(let bytes):
            return bytes.description
        }
    }

    var count: Int {
        switch self {
        case .string(let string):
            return string.count
        case .rawBytes(let rawBytes):
            return strnlen(rawBytes, Int.max)
        }
    }

    func hash(into hasher: inout Hasher) {
        // If Raw bytes is valid UTF8, hash it as so
        switch self {
        case .string(let string):
            hasher.combine(string)
        case .rawBytes(let bytes):
            if let stringValue = self.stringValue {
                hasher.combine(stringValue)
            } else {
                hasher.combine(bytes)
            }
        }
    }
}

extension FilePath {
    static var currentWorkingDirectory: Self {
        let path = getcwd(nil, 0)!
        defer { free(path) }
        return .init(String(cString: path))
    }
}

extension Optional where Wrapped: Collection {
    func withOptionalUnsafeBufferPointer<Result>(
        _ body: ((UnsafeBufferPointer<Wrapped.Element>)?) throws -> Result
    ) rethrows -> Result {
        switch self {
        case .some(let wrapped):
            guard let array: [Wrapped.Element] = wrapped as? Array else {
                return try body(nil)
            }
            return try array.withUnsafeBufferPointer { ptr in
                return try body(ptr)
            }
        case .none:
            return try body(nil)
        }
    }
}

extension Optional where Wrapped == String {
    func withOptionalCString<Result>(
        _ body: ((UnsafePointer<Int8>)?) throws -> Result
    ) rethrows -> Result {
        switch self {
        case .none:
            return try body(nil)
        case .some(let wrapped):
            return try wrapped.withCString {
                return try body($0)
            }
        }
    }

    var stringValue: String {
        return self ?? "nil"
    }
}

// MARK: - Stubs for the one from Foundation
public enum QualityOfService: Int, Sendable {
    case userInteractive = 0x21
    case userInitiated = 0x19
    case utility = 0x11
    case background = 0x09
    case `default` = -1
}

internal struct PipeCreator : ~Copyable {
    let read: TrackedFileDescriptor?
    let write: TrackedFileDescriptor?
    
    /// A pipe, Customized for the `NoInput` type.
    init(_ i: NoInput) throws {
        // work around "Conditional initialization or destruction of noncopyable types is not supported"
        let r: TrackedFileDescriptor?
        let w: TrackedFileDescriptor?
        
        r = try i.createReadFileDescriptor()
        w = try i.createWriteFileDescriptor()
        
        read = r
        write = w
    }
    
    /// A pipe, Customized for the `FileDescriptorInput` type.
    init(_ i: FileDescriptorInput) throws {
        let r: TrackedFileDescriptor?
        let w: TrackedFileDescriptor?
        
        r = try i.createReadFileDescriptor()
        w = try i.createWriteFileDescriptor()
        
        read = r
        write = w
    }
    
    #if SubprocessSpan
    @available(SubprocessSpan, *)
    #endif
    /// A pipe, Customized for the `DiscardedOutput` type.
    init(_ i: FileDescriptorOutput) throws {
        let r: TrackedFileDescriptor?
        let w: TrackedFileDescriptor?
        
        r = try i.createReadFileDescriptor()
        w = try i.createWriteFileDescriptor()
        
        read = r
        write = w
    }

    #if SubprocessSpan
    @available(SubprocessSpan, *)
    #endif
    /// A pipe, Customized for the `DiscardedOutput` type.
    init(_ i: DiscardedOutput) throws {
        let r: TrackedFileDescriptor?
        let w: TrackedFileDescriptor?
        
        r = try i.createReadFileDescriptor()
        w = try i.createWriteFileDescriptor()
        
        read = r
        write = w
    }
    
    /// A pipe for any other input type.
    init<I: InputProtocol>(_ i: I) throws {
        let pipe = try FileDescriptor.ssp_pipe()
        read = TrackedFileDescriptor(pipe.readEnd, closeWhenDone: true)
        write = TrackedFileDescriptor(pipe.writeEnd, closeWhenDone: true)
    }
    
    #if SubprocessSpan
    @available(SubprocessSpan, *)
    #endif
    /// A pipe for any other output type.
    init<I: OutputProtocol>(_ i: I) throws {
        let pipe = try FileDescriptor.ssp_pipe()
        read = TrackedFileDescriptor(pipe.readEnd, closeWhenDone: true)
        write = TrackedFileDescriptor(pipe.writeEnd, closeWhenDone: true)
    }
}
