

root := justfile_directory()
tester-path := root / '..' / 'redis-tester'
test-def := root / 'local' / 'tests.json'


test:
    cd {{tester-path}}; make build

    CODECRAFTERS_SUBMISSION_DIR={{root}} \
    CODECRAFTERS_TEST_CASES_JSON=`cat {{test-def}}` \
    {{tester-path}}/dist/main.out


lint:
    cargo fix --allow-dirty --allow-staged --all
