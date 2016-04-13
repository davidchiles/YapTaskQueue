//
//  YapTaskQueueErros.swift
//  YapTaskQueue
//
//  Created by David Chiles on 4/13/16.
//  Copyright © 2016 David Chiles. All rights reserved.
//

import Foundation


public enum YapTaskQueueError: ErrorType {
    case NoRegisteredViewName
    case CannotRegisterMasterView
    case CannotRegisterBrokerView
}