package nexssflow

import (
	"context"
	"errors"
	"sync"

	"github.com/nexssp/flow/compiler"
	"github.com/nexssp/kernel/action"
)

const ID = "db"

const registryMetaKey = "db.registry"

func init() {
	compiler.Register(ID, Bundle)
}

type bundle struct {
	mu         sync.Mutex
	registries []*registry
}

// Bundle returns the NFlow database actions and the @db declaration.
func Bundle(_ map[string]string) compiler.Bundle {
	b := &bundle{}
	return compiler.Bundle{
		ID:           ID,
		Libraries:    []action.Library{library()},
		Directives:   []compiler.Directive{databaseDirective(), databaseMappingDirective(), migrationDirective()},
		OnPreprocess: b.onPreprocess,
		InheritScope: inheritDatabaseRegistry,
		WrapPipeline: wrapPipeline,
		Shutdowns:    []compiler.ShutdownFunc{b.close},
	}
}

func (b *bundle) onPreprocess(meta map[string]any) compiler.PreprocessContributions {
	declarations := declarationsFromMeta(meta)
	mappings := tableMappingsFromMeta(meta)
	if (len(declarations) == 0 && len(mappings) == 0) || registryFromMeta(meta) != nil {
		return compiler.PreprocessContributions{}
	}

	store := newRegistry(declarations, mappings...)
	meta[registryMetaKey] = store
	b.mu.Lock()
	b.registries = append(b.registries, store)
	b.mu.Unlock()
	return compiler.PreprocessContributions{}
}

func (b *bundle) close(ctx context.Context) error {
	b.mu.Lock()
	registries := b.registries
	b.registries = nil
	b.mu.Unlock()

	var errs []error
	for _, store := range registries {
		if err := store.close(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func wrapPipeline(meta map[string]any, inner action.AnyAction) (action.AnyAction, error) {
	store := registryFromMeta(meta)
	if store == nil {
		return inner, nil
	}
	return action.New("db.wrap", func(ctx context.Context, req any) (any, error) {
		return action.InvokeAny(withRegistry(ctx, store), inner, req)
	}).Build(), nil
}

func inheritDatabaseRegistry(parentMeta, childMeta map[string]any) error {
	if store := registryFromMeta(parentMeta); store != nil {
		childMeta[registryMetaKey] = store
	}
	return nil
}

func registryFromMeta(meta map[string]any) *registry {
	store, _ := meta[registryMetaKey].(*registry)
	return store
}
