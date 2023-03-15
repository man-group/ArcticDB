function Quiet-Remove {
	param (
		$File
	)

	if (Test-Path $File) {
		Remove-Item $File
	}
}

Quiet-Remove .\lmdb++.h
New-Item -Path .\lmdb++.h -ItemType SymbolicLink -Value ..\lmdbxx\lmdb++.h

'.\lmdb.h', '.\mdb.c', '.\midl.h', '.\midl.c' |
ForEach {
	Quiet-Remove $PSItem
	New-Item -Path $PSItem -ItemType SymbolicLink -Value ..\lmdb\libraries\liblmdb\$PSItem
}
