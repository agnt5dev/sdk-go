package agnt5

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)

const envDashboardURL = "AGNT5_DASHBOARD_URL"

type startupComponentGroup struct {
	componentType ComponentType
	icon          string
	label         string
}

var startupComponentGroups = []startupComponentGroup{
	{componentType: ComponentTypeWorkflow, icon: "◆", label: "workflows"},
	{componentType: ComponentTypeFunction, icon: "ƒ", label: "functions"},
	{componentType: ComponentTypeAgent, icon: "●", label: "agents"},
	{componentType: ComponentTypeTool, icon: "◇", label: "tools"},
	{componentType: ComponentTypeScorer, icon: "★", label: "scorers"},
	{componentType: ComponentTypeChat, icon: "•", label: "chats"},
	{componentType: ComponentTypeMCP, icon: "•", label: "MCPs"},
	{componentType: ComponentTypeEntity, icon: "•", label: "entities"},
	{componentType: ComponentTypeRun, icon: "•", label: "runs"},
}

func (w *Worker) printStartupBanner() {
	output := w.startupOutput()
	componentsByType := make(map[ComponentType][]string)
	for _, component := range w.Components() {
		componentsByType[component.Type] = append(componentsByType[component.Type], component.Name)
	}

	fmt.Fprintf(output, "\n  %s v%s\n", w.serviceName, w.serviceVersion)
	fmt.Fprintln(output, "  "+strings.Repeat("─", 40))

	for _, group := range startupComponentGroups {
		printStartupComponentGroup(output, group, componentsByType[group.componentType])
		delete(componentsByType, group.componentType)
	}

	remainingTypes := make([]string, 0, len(componentsByType))
	for componentType := range componentsByType {
		remainingTypes = append(remainingTypes, string(componentType))
	}
	sort.Strings(remainingTypes)
	for _, typeName := range remainingTypes {
		printStartupComponentGroup(output, startupComponentGroup{
			componentType: ComponentType(typeName),
			icon:          "•",
			label:         typeName + "s",
		}, componentsByType[ComponentType(typeName)])
	}

	fmt.Fprintln(output, "  "+strings.Repeat("─", 40))
	if dashboardURL := strings.TrimSpace(os.Getenv(envDashboardURL)); dashboardURL != "" {
		fmt.Fprintf(output, "  Dashboard: %s\n", dashboardURL)
	}
	fmt.Fprintln(output)
}

func printStartupComponentGroup(output io.Writer, group startupComponentGroup, names []string) {
	if len(names) == 0 {
		return
	}
	sort.Strings(names)
	fmt.Fprintf(output, "  %s %s (%d)\n", group.icon, group.label, len(names))
	for index, name := range names {
		prefix := "├──"
		if index == len(names)-1 {
			prefix = "└──"
		}
		fmt.Fprintf(output, "    %s %s\n", prefix, name)
	}
}

func (w *Worker) printConnecting() {
	fmt.Fprintf(w.startupOutput(), "Connecting to coordinator (%s)...\n", w.coordinatorEndpoint)
}

func (w *Worker) printConnected() {
	fmt.Fprintf(w.startupOutput(), "Connected to coordinator (%s)\n", w.coordinatorEndpoint)
}

func (w *Worker) startupOutput() io.Writer {
	if w.startupWriter != nil {
		return w.startupWriter
	}
	return io.Discard
}
