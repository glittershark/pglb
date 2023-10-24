#![allow(dead_code)]

use num_derive::FromPrimitive;

#[derive(Debug, Clone, Copy, FromPrimitive)]
#[repr(u8)]
pub enum FrontendMessageType {
    Startup = 0,
    CancelRequest = 1,
    GSSENCRequest = 2,
    SSLRequest = 3,

    Bind = b'B',
    Close = b'C',
    CopyData = b'd',
    CopyDone = b'c',
    CopyFail = b'f',
    Describe = b'D',
    Execute = b'E',
    Flush = b'H',
    FunctionCall = b'F',
    Parse = b'P',
    Password = b'p',
    Query = b'Q',
    Sync = b'S',
    Terminate = b'X',
}

#[derive(Debug, Clone, Copy, FromPrimitive)]
#[repr(u8)]
pub enum BackendMessageType {
    Startup = 0,
    CancelRequest = 1,

    Authentication = b'R',
    BackendKeyData = b'K',
    BindComplete = b'2',
    CloseComplete = b'3',
    CommandComplete = b'C',
    CopyData = b'd',
    CopyDone = b'c',
    CopyInResponse = b'G',
    CopyOutResponse = b'H',
    CopyBothResponse = b'W',
    DataRow = b'D',
    EmptyQueryResponse = b'I',
    ErrorResponse = b'E',
    FunctionCallResponse = b'V',
    NegotiateProtocolVersion = b'v',
    NoData = b'n',
    NoticeResponse = b'N',
    NotificationResponse = b'A',
    ParameterDescription = b't',
    ParameterStatus = b'S',
    ParseComplete = b'1',
    PortalSuspended = b's',
    ReadyForQuery = b'Z',
    RowDescription = b'T',
}
