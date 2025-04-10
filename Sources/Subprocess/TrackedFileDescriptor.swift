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
import System
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

internal import Dispatch

/// A simple wrapper on `FileDescriptor` plus a flag indicating
/// whether it should be closed automactially when done.
internal struct TrackedFileDescriptor: ~Copyable {
    private var closeWhenDone: Bool
    
    /// Access the file descriptor directly.
    // TODO: In the long term, it would be nice to abstract this completely behind this type, so we do not need to worry about the `fd` escaping the ~Copyable behavior. But for now, a lot of code relies on getting this value directly.
    internal let platformDescriptor: FileDescriptor

    internal init(
        _ platformDescriptor: FileDescriptor,
        closeWhenDone: Bool
    ) {
        self.platformDescriptor = platformDescriptor
        self.closeWhenDone = closeWhenDone
    }

    consuming internal func safelyClose() throws {
        guard self.closeWhenDone else {
            return
        }

        do {
            try self.platformDescriptor.close()
        } catch {
            guard let errno: Errno = error as? Errno else {
                throw error
            }
            if errno != .badFileDescriptor {
                throw errno
            }
        }
    }
    
    consuming internal func extractPlatformDescriptor() -> (FileDescriptor, Bool) {
        let result = (platformDescriptor, closeWhenDone)
        closeWhenDone = false
        return result
    }
    
    deinit {
        if closeWhenDone {
            try? self.platformDescriptor.close()
        }
    }
}
