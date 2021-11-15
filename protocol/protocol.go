package protocol

const (
	DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE          = 54058
	DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO = 54060
)

const (
	ClientHello = iota
	ClientQuery
	ClientData
	ClientCancel
	ClientPing
)

const (
	ServerHello = iota
	ServerData
	ServerException
	ServerProgress
	ServerPong
	ServerEndOfStream
	ServerProfileInfo
	ServerTotals
	ServerExtremes
)
