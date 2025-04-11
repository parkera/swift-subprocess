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
#elseif canImport(Bionic)
import Bionic
#elseif canImport(Glibc)
import Glibc
#elseif canImport(Musl)
import Musl
#elseif canImport(WinSDK)
import WinSDK
#endif

/// An object that repersents a subprocess that has been
/// executed. You can use this object to send signals to the
/// child process as well as stream its output and error.
#if SubprocessSpan
@available(SubprocessSpan, *)
#endif
public struct Execution<
    Output: OutputProtocol,
    Error: OutputProtocol
>: Sendable, ~Copyable {
    /// The process identifier of the current execution
    public let processIdentifier: ProcessIdentifier

    internal let output: Output
    internal let error: Error
    internal var outputRead: TrackedFileDescriptor?
    internal var errorRead: TrackedFileDescriptor?
    
    #if os(Windows)
    internal let consoleBehavior: PlatformOptions.ConsoleBehavior

    fileprivate init(
        processIdentifier: ProcessIdentifier,
        output: Output,
        error: Error,
        outputPipe: CreatedPipe,
        errorPipe: CreatedPipe,
        consoleBehavior: PlatformOptions.ConsoleBehavior
    ) {
        self.processIdentifier = processIdentifier
        self.output = output
        self.error = error
        self.outputPipe = outputPipe
        self.errorPipe = errorPipe
        self.outputConsumptionState = AtomicBox()
        self.consoleBehavior = consoleBehavior
    }
    #else
    init(
        processIdentifier: ProcessIdentifier,
        output: Output,
        error: Error,
        outputRead: consuming TrackedFileDescriptor?,
        errorRead: consuming TrackedFileDescriptor?,
    ) {
        self.processIdentifier = processIdentifier
        self.output = output
        self.error = error
        self.outputRead = consume outputRead
        self.errorRead = consume errorRead
    }
    #endif  // os(Windows)
}

#if SubprocessSpan
@available(SubprocessSpan, *)
#endif
extension Execution where Output == SequenceOutput {
    /// The standard output of the subprocess.
    /// Accessing this property will assert if property was accessed multiple times. Subprocess communicates with parent process via a pipe, and each pipe can only be consumed once.
    public var standardOutput: some AsyncSequence<SequenceOutput.Buffer, any Swift.Error> {
        mutating get {
            let result: AsyncBufferSequence
            if let outputRead {
                result = AsyncBufferSequence(fileDescriptor: outputRead)
            } else {
                fatalError("Execution.standardOutput was accessed more than once")
            }
            self = .init(processIdentifier: processIdentifier, output: output, error: error, outputRead: nil, errorRead: errorRead)
            return result
        }
    }
}

#if SubprocessSpan
@available(SubprocessSpan, *)
#endif
extension Execution where Error == SequenceOutput {
    /// The standard error of the subprocess.
    /// Accessing this property will assert if property was accessed multiple times. Subprocess communicates with parent process via a pipe, and each pipe can only be consumed once.
    public var standardError: some AsyncSequence<SequenceOutput.Buffer, any Swift.Error> {
        mutating get {
            let result: AsyncBufferSequence
            if let errorRead {
                result = AsyncBufferSequence(fileDescriptor: errorRead)
            } else {
                fatalError("Execution.standardError was accessed more than once")
            }
            self = .init(processIdentifier: processIdentifier, output: output, error: error, outputRead: outputRead, errorRead: nil)
            return result
        }
    }
}

// MARK: - Output Capture
internal enum OutputCapturingState<Output: Sendable, Error: Sendable>: Sendable {
    case standardOutputCaptured(Output)
    case standardErrorCaptured(Error)
}

internal struct OutputConsumptionState: OptionSet {
    typealias RawValue = UInt8

    internal let rawValue: UInt8

    internal init(rawValue: UInt8) {
        self.rawValue = rawValue
    }

    static let standardOutputConsumed: Self = .init(rawValue: 0b0001)
    static let standardErrorConsumed: Self = .init(rawValue: 0b0010)
}

internal typealias CapturedIOs<
    Output: Sendable,
    Error: Sendable
> = (standardOutput: Output, standardError: Error)

#if SubprocessSpan
@available(SubprocessSpan, *)
#endif
extension Execution {
    /// Consume the output read and error read file descriptors, turning them into the stdout/stderrr types.
    consuming internal func captureIOs() async throws -> CapturedIOs<
        Output.OutputType, Error.OutputType
    > {
        let standardOutput = output
        let standardError = error
        
        // Wrap the ~Copyable file descriptors in an Optional so we can pass them to a closure which executes once. There is no way to tell the compiler that the closure is only executed once, so this moves that check to a dynamic one (at the force unwrap below).
        var readR : TrackedFileDescriptor? = outputRead
        var errorR : TrackedFileDescriptor? = errorRead
        self = .init(processIdentifier: processIdentifier, output: output, error: error, outputRead: nil, errorRead: nil)
        return try await withThrowingTaskGroup(
            of: OutputCapturingState<Output.OutputType, Error.OutputType>.self
        ) { group in
            // 2nd layer of moving noncopyable types into a wrapper type
            var readRR = readR.take()
            var errorRR = errorR.take()
            group.addTask {
                let r = readRR.take()!
                let stdout = try await standardOutput.captureOutput(from: r)
                return .standardOutputCaptured(stdout)
            }
            group.addTask {
                let e = errorRR.take()!
                let stderr = try await standardError.captureOutput(from: e)
                return .standardErrorCaptured(stderr)
            }

            var stdout: Output.OutputType!
            var stderror: Error.OutputType!
            while let state = try await group.next() {
                switch state {
                case .standardOutputCaptured(let output):
                    stdout = output
                case .standardErrorCaptured(let error):
                    stderror = error
                }
            }
            return (
                standardOutput: stdout,
                standardError: stderror
            )
        }
    }
}
