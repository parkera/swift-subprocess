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

#if SubprocessSpan
@available(SubprocessSpan, *)
#endif
public struct AsyncBufferSequence: AsyncSequence, Sendable {
    public typealias Failure = any Swift.Error

    public typealias Element = Buffer

    @_nonSendable
    public struct Iterator: AsyncIteratorProtocol {
        public typealias Element = AsyncBufferSequence.Buffer

        private let fileDescriptor: PlatformFileDescriptor
        private var buffer: [UInt8]
        private var currentPosition: Int
        private var finished: Bool
        private var closeWhenDone: Bool

        internal init(fileDescriptor: PlatformFileDescriptor, closeWhenDone: Bool) {
            self.fileDescriptor = fileDescriptor
            self.buffer = []
            self.currentPosition = 0
            self.finished = false
            self.closeWhenDone = closeWhenDone
        }

        public mutating func next() async throws -> AsyncBufferSequence.Buffer? {
            let data = try await self.fileDescriptor.readChunk(
                upToLength: readBufferSize
            )
            if data == nil {
                // We finished reading. Close the file descriptor now
                if closeWhenDone {
                    try? fileDescriptor.close()
                }
                return nil
            }
            return data
        }
    }

    private let fileDescriptor: PlatformFileDescriptor
    private let closeWhenDone: Bool

    init(fileDescriptor: consuming TrackedFileDescriptor) {
        // Maybe someday the sequence itself could be non-copyable, but we can't do it yet. Still, we consume the TrackedFileDescriptor to ensure it is properly closed.
        (self.fileDescriptor, closeWhenDone) = fileDescriptor.extractPlatformDescriptor()
    }

    public func makeAsyncIterator() -> Iterator {
        return Iterator(fileDescriptor: fileDescriptor, closeWhenDone: closeWhenDone)
    }
}

// MARK: - Page Size
import _SubprocessCShims

#if canImport(Darwin)
import Darwin
internal import MachO.dyld

private let _pageSize: Int = {
    Int(_subprocess_vm_size())
}()
#elseif canImport(WinSDK)
import WinSDK
private let _pageSize: Int = {
    var sysInfo: SYSTEM_INFO = SYSTEM_INFO()
    GetSystemInfo(&sysInfo)
    return Int(sysInfo.dwPageSize)
}()
#elseif os(WASI)
// WebAssembly defines a fixed page size
private let _pageSize: Int = 65_536
#elseif canImport(Android)
@preconcurrency import Android
private let _pageSize: Int = Int(getpagesize())
#elseif canImport(Glibc)
@preconcurrency import Glibc
private let _pageSize: Int = Int(getpagesize())
#elseif canImport(Musl)
@preconcurrency import Musl
private let _pageSize: Int = Int(getpagesize())
#elseif canImport(C)
private let _pageSize: Int = Int(getpagesize())
#endif  // canImport(Darwin)

@inline(__always)
internal var readBufferSize: Int {
    return _pageSize
}
